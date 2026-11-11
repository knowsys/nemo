//! Fetch rules: the rules of bound SPARQL imports, applied once per iteration like the other
//! rules.
//!
//! A bound import `@import q(?x, ?y) from sparql { … } where b(?x), c(?y)`
//! compiles to a rule whose body atoms are the *binding atoms* and whose head is `q`.
//! Applying it queries the endpoint instead of joining.
//! The values of the binding atoms' rows are sent as [ValuesBlock]s of the query,
//! and the solutions become the head's new rows.
//! The evaluation is semi-naive, as for the other rules (see [requests]).
//!
//! The engine does not send the requests itself.
//! An application whose requests have no responses yet is suspended as a [PendingFetch],
//! and completes once the responses have been passed in,
//! through [resume](crate::engine::Engine::resume) or by the engine's client.

use std::{collections::HashSet, ops::Range};

use nemo_io::{
    remote::Request,
    sparql::{self, ValuesBlock},
};
use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{ColumnOrder, Revision, TableId, TableManager},
    plan::{Marker, OperationPlan},
    storage::StorageValue,
    trie::{RowBuffer, Trie},
    value::{DataValue, ErrorCounts, ValueRef},
};

use crate::{
    diagnostics::AboutResource,
    engine::{
        compile::compiled::{BodyAtom, CompiledFetch, CompiledRule, FetchColumn},
        evaluate::{application::Applied, body::BodySpace},
        remote::RequestSlots,
    },
    model::OnError,
};

/// The query length in bytes beyond which a request is built again with a binding page half as
/// wide.
///
/// It stays well under the size of a POST body that endpoints accept.
const QUERY_CHAR_LIMIT: usize = 500_000;

/// The `VALUES` blocks of one request.
///
/// If the endpoint finds the request too large, or cuts its response off, its widest block is
/// halved.
type ValuesBlocks = Vec<ValuesBlock>;

/// Per binding atom, its rows of bound values, as a family of requests sends them.
type BindingRows = Vec<Vec<Vec<DataValue>>>;

/// The requests of one fetch application, each with its `VALUES` blocks.
struct ApplicationRequests {
    /// The requests.
    requests: Vec<(Request, ValuesBlocks)>,
    /// The number of new binding rows left out because they contain a blank node that an
    /// endpoint returned.
    blank_bindings: u64,
}

/// The rows of bound values of one family of requests.
struct FamilyBindings {
    /// Per binding atom, its rows.
    rows: BindingRows,
    /// The number of new keys of the pivot atom left out because they contain a blank node
    /// that an endpoint returned.
    blank_bindings: u64,
}

/// What one fetch application did beyond deriving rows.
#[derive(Debug, Default, Clone)]
pub(super) struct Fetched {
    /// The number of requests made, including the halves of split requests.
    pub(super) requests: u64,
    /// The number of solution rows received, before deduplication.
    pub(super) rows: u64,
    /// The number of rows in the responses that were not valid solutions.
    pub(super) malformed: u64,
    /// The number of solution rows on which a condition pushed into the import failed,
    /// per error kind.
    ///
    /// They are dropped, as the rule would have dropped them.
    pub(super) errors: ErrorCounts,
    /// The number of requests that failed and were skipped under `on_error = warn`.
    pub(super) failed: u64,
    /// The number of binding combinations that the failed requests carried, which are
    /// not queried again.
    pub(super) unasked: u64,
    /// Why the first failed request failed.
    pub(super) first_failure: Option<String>,
    /// The number of new binding rows left out because they contain a blank node that an
    /// endpoint returned, which no query can name.
    pub(super) blank_bindings: u64,
}

/// A fetch application suspended until its requests have responses:
/// the rule, the step of the application,
/// and the requests, each with its `VALUES` blocks.
#[derive(Debug)]
pub(in crate::engine) struct PendingFetch {
    /// The rule's index.
    pub(in crate::engine) rule: usize,
    /// The step of the application.
    pub(in crate::engine) step: u64,
    /// The requests, each with its `VALUES` blocks.
    pub(in crate::engine) slots: RequestSlots<ValuesBlocks>,
    /// The number of requests made so far, including the halves of split requests.
    made: u64,
    /// The number of new binding rows left out because they contain a blank node that an
    /// endpoint returned.
    blank_bindings: u64,
}

/// How a fetch application ended: applied, or suspended until its requests have responses.
#[derive(Debug)]
pub(super) enum FetchOutcome {
    /// The rule was applied, with what it derived and what it did.
    Applied(Box<Applied>, Fetched),
    /// The application is suspended until the requests of this pending fetch have responses.
    Suspended(PendingFetch),
}

/// Applies a fetch rule at the given step.
///
/// The binding atoms' rows since the rule's last application are sent to the endpoint,
/// and the solutions become the head table's new rows,
/// without the rows it already stores.
///
/// The first time, the application builds its requests and is suspended until they have
/// responses.
/// When it continues with them (`resumed`), it splits the requests that the endpoint could not
/// respond to in full (see [split_too_large](PendingFetch::split_too_large)),
/// is suspended again for the halves, and then reads every response.
/// A failed request fails the application, or, under `on_error = warn`, is counted and skipped.
/// The import's conditions are evaluated in `context`, the run's context.
/// `index` is the rule's index, and `ids` the last request id given, which this advances.
#[expect(clippy::too_many_arguments, reason = "the parts of one application")]
pub(super) fn apply(
    rule: &CompiledRule,
    index: usize,
    step: u64,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
    resumed: Option<PendingFetch>,
    ids: &mut u64,
) -> Result<FetchOutcome, Diagnostic> {
    let fetch = rule.fetch.as_ref().expect("a fetch rule carries its fetch");
    let head = rule.heads.first().expect("a fetch rule has its head");
    let arity = head.columns.len();

    let mut pending = match resumed {
        Some(pending) => {
            assert!(
                pending.rule == index && pending.step == step,
                "a fetch resumes at the application it was suspended in"
            );
            pending
        }
        None => {
            let ApplicationRequests {
                requests,
                blank_bindings,
            } = requests(rule, fetch, step, manager, dictionary, context)?;
            if requests.is_empty() {
                let fetched = Fetched {
                    blank_bindings,
                    ..Fetched::default()
                };
                return Ok(FetchOutcome::Applied(Box::default(), fetched));
            }
            let mut pending = PendingFetch {
                rule: index,
                step,
                slots: RequestSlots::default(),
                made: 0,
                blank_bindings,
            };
            for (request, blocks) in requests {
                pending.slots.push(request, blocks, ids);
                pending.made += 1;
            }
            tracing::debug!(
                endpoint = fetch.endpoint,
                requests = pending.made,
                step,
                "fetch application suspended for its responses"
            );
            pending
        }
    };
    pending.split_too_large(fetch, ids);
    if pending.slots.has_pending() {
        return Ok(FetchOutcome::Suspended(pending));
    }

    let (buffer, fetched) = solutions(fetch, arity, pending, dictionary, context)?;
    let mut applied = Applied {
        errors: fetched.errors,
        ..Applied::default()
    };
    if buffer.is_empty() {
        return Ok(FetchOutcome::Applied(Box::new(applied), fetched));
    }

    // Deduplicate against the head. The batch is stored in the scratch
    // table at this step, and what the application derived is the batch
    // minus the stored rows.
    let fresh = subtract_stored(
        fetch.scratch,
        head.table,
        step,
        buffer,
        manager,
        dictionary,
        context,
    );
    // Drop the scratch rows, whether or not the subtraction failed.
    manager.drop_rows(fetch.scratch, Revision(step)..Revision(step + 1));
    let fresh = fresh?;
    if !fresh.is_empty() {
        applied.outputs.push((head.table, fresh));
    }
    Ok(FetchOutcome::Applied(Box::new(applied), fetched))
}

impl PendingFetch {
    /// Replaces every request that the endpoint could not respond to in full by two requests,
    /// each with one half of its widest `VALUES` block.
    ///
    /// The endpoint could not respond to a request in full if:
    /// - it found the request too large (status 413 or 414),
    /// - it cut the response off partway, typically at its time limit, or
    /// - it responded only in part, and said so in its status or headers
    ///   ([Failure::of_partial_response](nemo_io::remote::Failure::of_partial_response)).
    ///
    /// A request whose blocks all have a single row keeps its response.
    /// `ids` is the last request id given, which this advances.
    fn split_too_large(&mut self, fetch: &CompiledFetch, ids: &mut u64) {
        let slots = std::mem::take(&mut self.slots.0);
        for slot in slots {
            let too_large = match &slot.response {
                Some(Err(failure)) => failure.smaller_may_succeed(),
                Some(Ok(body)) => sparql::SparqlReader::is_cut_off(body),
                None => false,
            };
            let widest = slot
                .key
                .iter()
                .enumerate()
                .filter(|(_, block)| block.rows.len() > 1)
                .max_by_key(|(_, block)| block.rows.len())
                .map(|(position, _)| position);
            let (true, Some(widest)) = (too_large, widest) else {
                self.slots.0.push(slot);
                continue;
            };
            let mut first = slot.key;
            let half = first[widest].rows.len() / 2;
            let rest = first[widest].rows.split_off(half);
            let mut second = first.clone();
            second[widest].rows = rest;
            tracing::debug!(
                endpoint = fetch.endpoint,
                rows = half,
                "an endpoint could not answer a request whole; halving it"
            );
            for blocks in [first, second] {
                self.slots.push(request_for(fetch, &blocks), blocks, ids);
                self.made += 1;
            }
        }
    }
}

/// Returns the requests of one application.
///
/// There is one *family* of requests per binding atom, which is the family's *pivot*.
/// The pivot reads its delta since the rule's last application,
/// the atoms before it read their rows as of that application,
/// and the atoms after it read their rows as of now.
/// So every combination of binding rows is queried exactly once,
/// and over all applications, the requests cover every binding that the binding atoms derive.
/// A solution row is thus fetched once too,
/// which keeps the nulls created for its blank nodes stable.
/// A family with an empty input sends no request.
///
/// It also counts the new binding rows left out because they contain a blank node that an
/// endpoint returned.
fn requests(
    rule: &CompiledRule,
    fetch: &CompiledFetch,
    step: u64,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<ApplicationRequests, Diagnostic> {
    let last = Revision(rule.last_applied);
    let now = Revision(step);
    let count = rule.body.len();
    let mut requests = Vec::new();
    let mut blank_bindings = 0;
    for pivot in 0..count {
        let ranges: Vec<Range<Revision>> = (0..count)
            .map(|position| match position.cmp(&pivot) {
                std::cmp::Ordering::Less => Revision(0)..last,
                std::cmp::Ordering::Equal => last..now,
                std::cmp::Ordering::Greater => Revision(0)..now,
            })
            .collect();
        let mut empty = false;
        for (atom, range) in rule.body.iter().zip(&ranges) {
            if manager.row_count(atom.table, range.clone()) == 0 {
                empty = true;
                break;
            }
        }
        if empty {
            continue;
        }
        let FamilyBindings {
            rows,
            blank_bindings: blank,
        } = binding_rows(rule, fetch, &ranges, pivot, manager, dictionary, context)?;
        blank_bindings += blank;
        if rows.iter().any(Vec::is_empty) {
            continue;
        }
        requests.extend(paged_requests(fetch, &rows));
    }
    Ok(ApplicationRequests {
        requests,
        blank_bindings,
    })
}

/// Returns the solution rows of every response,
/// read through the fetch's selection into one batch of the head's arity,
/// with the fetch's statistics.
///
/// Only the solutions that match a request's binding rows, as Nemo compares values, are kept
/// ([with_values](nemo_io::sparql::SparqlReader::with_values)).
/// A failed request, or a response that cannot be read, fails the application,
/// unless the import sets `on_error = warn`.
fn solutions(
    fetch: &CompiledFetch,
    arity: usize,
    pending: PendingFetch,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<(RowBuffer, Fetched), Diagnostic> {
    let written = fetch.query.width();
    let mut buffer = RowBuffer::new(arity);
    let mut fetched = Fetched {
        requests: pending.made,
        blank_bindings: pending.blank_bindings,
        ..Fetched::default()
    };
    for slot in pending.slots.0 {
        let reader = sparql::SparqlReader::new(written, fetch.selection.clone())
            .with_values(&slot.key)
            .with_endpoint_blank_nodes();
        let response = slot
            .response
            .expect("a fetch reads the responses once every request has one");
        let loaded = response
            .map_err(|failure| failure.diagnostic(&fetch.endpoint))
            .and_then(|bytes| reader.read(&fetch.endpoint, &bytes, dictionary, context));
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(problem) if fetch.on_error == OnError::Warn => {
                fetched.failed += 1;
                fetched.unasked = fetched.unasked.saturating_add(combinations(&slot.key));
                fetched
                    .first_failure
                    .get_or_insert_with(|| problem.message().to_owned());
                continue;
            }
            Err(problem) => {
                let problem = problem.with_resource(fetch.endpoint.clone());
                return Err(match fetch.span {
                    Some(span) => problem.with_label(span, "requested by this import"),
                    None => problem,
                });
            }
        };
        fetched.malformed += loaded.malformed;
        fetched.errors.absorb(&loaded.errors);
        fetched.rows += loaded.rows.len() as u64;
        for index in 0..loaded.rows.len() {
            buffer.push_row(loaded.rows.row(index));
        }
    }
    Ok((buffer, fetched))
}

/// Returns the number of binding combinations that a request queries:
/// the product of its blocks' row counts.
fn combinations(blocks: &ValuesBlocks) -> u64 {
    blocks.iter().fold(1u64, |product, block| {
        product.saturating_mul(block.rows.len() as u64)
    })
}

/// Returns the request that queries the bindings of the given `VALUES` blocks.
fn request_for(fetch: &CompiledFetch, blocks: &[ValuesBlock]) -> Request {
    request_with(fetch, &render(fetch, blocks))
}

/// Returns the text of the fetch's query with the given `VALUES` blocks.
fn render(fetch: &CompiledFetch, blocks: &[ValuesBlock]) -> String {
    // Translation binds only queries that are not opaque, as `with_values` requires.
    fetch.query.with_values(blocks).text().to_owned()
}

/// Returns the request that sends a rendered query to the fetch's endpoint.
fn request_with(fetch: &CompiledFetch, text: &str) -> Request {
    let mut request = Request::sparql(&fetch.endpoint, text);
    request.headers.extend(fetch.headers.iter().cloned());
    request
}

/// Returns the rows of a batch that the head table does not store before `step`.
///
/// The batch is added to the `scratch` table at `step`, where it stays.
fn subtract_stored(
    scratch: TableId,
    head: TableId,
    step: u64,
    batch: RowBuffer,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Trie, Diagnostic> {
    let arity = batch.arity();
    manager.add_rows(scratch, Revision(step), ColumnOrder::identity(arity), batch);
    let mut plan = OperationPlan::new();
    let markers: Vec<Marker> = (0..arity).map(|_| plan.marker()).collect();
    let batch = plan.load(scratch, Revision(step)..Revision(step + 1), markers.clone());
    let stored = plan.load(head, Revision(0)..Revision(step), markers.clone());
    let fresh = plan.subtract(batch, stored);
    let output = plan.output(fresh);
    let mut results = plan.execute(manager, dictionary, context);
    Ok(std::mem::take(&mut results.outputs[output]))
}

/// Adds the rows of one binding atom within a revision range to the plan as an output,
/// and returns the output's index.
///
/// The rows are in the atom's column order, under the atom's own constraints
/// (its constants and repetitions).
/// The endpoint joins the atoms, not the plan.
fn atom_rows(
    plan: &mut OperationPlan,
    space: &BodySpace<'_>,
    atom: &BodyAtom,
    range: Range<Revision>,
) -> Result<usize, Diagnostic> {
    let atom_markers = space.atom_markers(atom);
    let load = plan.load(atom.table, range, atom_markers.clone());
    let node = space.join(plan, vec![load], atom_markers, &[]);
    Ok(plan.output(node))
}

/// Returns, per binding atom, the distinct rows of its bound columns within its revision range,
/// decoded into values.
///
/// A row's *key* is its values in the bound columns.
/// A row with a value that has no ground term, such as a null, is left out,
/// since no SPARQL query can name it.
/// The pivot atom reads its delta,
/// and a key that the pivot already had before the delta is left out too,
/// since an earlier application queried it already.
///
/// It also counts the pivot's new keys that were left out because they contain a blank node
/// that an endpoint returned.
/// Across a run, each key is new to one application only.
fn binding_rows(
    rule: &CompiledRule,
    fetch: &CompiledFetch,
    ranges: &[Range<Revision>],
    pivot: usize,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<FamilyBindings, Diagnostic> {
    let mut plan = OperationPlan::new();
    let space = BodySpace::new(&mut plan, rule);
    let mut outputs = Vec::with_capacity(rule.body.len());
    // The pivot's rows before its delta, whose keys were queried already.
    let mut asked = None;
    for (index, (atom, range)) in rule.body.iter().zip(ranges).enumerate() {
        outputs.push(atom_rows(&mut plan, &space, atom, range.clone())?);
        if index == pivot && range.start > Revision(0) {
            asked = Some(atom_rows(
                &mut plan,
                &space,
                atom,
                Revision(0)..range.start,
            )?);
        }
    }
    let mut results = plan.execute(manager, dictionary, context);

    let mut per_atom = Vec::with_capacity(rule.body.len());
    let mut blank = 0;
    for (index, columns) in fetch.atoms.iter().enumerate() {
        let bound: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| matches!(column, FetchColumn::Bound(_)))
            .map(|(column, _)| column)
            .collect();
        let trie = std::mem::take(&mut results.outputs[outputs[index]]);
        let mut seen: HashSet<Vec<StorageValue>> = HashSet::new();
        if index == pivot
            && let Some(asked) = asked
        {
            std::mem::take(&mut results.outputs[asked]).for_each_row(|row| {
                seen.insert(bound.iter().map(|&column| row[column]).collect());
            });
        }
        let mut rows = Vec::new();
        trie.for_each_row(|row| {
            let key: Vec<StorageValue> = bound.iter().map(|&column| row[column]).collect();
            if !seen.insert(key.clone()) {
                return;
            }
            let mut values = Vec::with_capacity(key.len());
            for value in key {
                let decoded = dictionary
                    .decode(value)
                    .expect("stored values decode against their dictionary")
                    .to_owned();
                if sparql::SparqlQuery::ground_term(&decoded).is_none() {
                    if index == pivot
                        && let ValueRef::Null(id) = decoded.as_ref()
                        && sparql::SparqlReader::is_endpoint_blank_node(dictionary, id)
                    {
                        blank += 1;
                    }
                    return;
                }
                values.push(decoded);
            }
            rows.push(values);
        });
        per_atom.push(rows);
    }
    Ok(FamilyBindings {
        rows: per_atom,
        blank_bindings: blank,
    })
}

/// Returns the requests that query one family of bindings.
///
/// Every atom's rows are split into pages, each page is a `VALUES` block,
/// and there is one request per combination of pages.
/// If a rendered query is longer than [QUERY_CHAR_LIMIT],
/// the widest page is halved and the requests are built again.
fn paged_requests(
    fetch: &CompiledFetch,
    rows: &[Vec<Vec<DataValue>>],
) -> Vec<(Request, ValuesBlocks)> {
    let count = rows.len();
    let positions: Vec<Vec<usize>> = fetch
        .atoms
        .iter()
        .map(|columns| {
            columns
                .iter()
                .filter_map(|column| match column {
                    FetchColumn::Bound(position) => Some(*position),
                    _ => None,
                })
                .collect()
        })
        .collect();
    let mut pages = vec![fetch.page_size.max(1); count];
    loop {
        let chunks: Vec<Vec<&[Vec<DataValue>]>> = rows
            .iter()
            .zip(&pages)
            .map(|(rows, &page)| rows.chunks(page).collect())
            .collect();
        let mut requests = Vec::new();
        let mut indices = vec![0usize; count];
        let mut too_long = false;
        'product: loop {
            let blocks: ValuesBlocks = (0..count)
                .filter(|&atom| !positions[atom].is_empty())
                .map(|atom| ValuesBlock {
                    positions: positions[atom].clone(),
                    rows: chunks[atom][indices[atom]].to_vec(),
                })
                .collect();
            let text = render(fetch, &blocks);
            if text.len() > QUERY_CHAR_LIMIT && pages.iter().any(|&page| page > 1) {
                too_long = true;
                break;
            }
            requests.push((request_with(fetch, &text), blocks));
            // Advance to the next combination, the first atom's page fastest.
            let mut atom = 0;
            loop {
                if atom == count {
                    break 'product;
                }
                indices[atom] += 1;
                if indices[atom] < chunks[atom].len() {
                    break;
                }
                indices[atom] = 0;
                atom += 1;
            }
        }
        if !too_long {
            return requests;
        }
        // Halve the widest page that can still shrink.
        let widest = (0..count)
            .filter(|&atom| pages[atom] > 1)
            .max_by_key(|&atom| pages[atom].min(rows[atom].len()))
            .expect("a request is only too long while a page is wider than one");
        pages[widest] /= 2;
    }
}
