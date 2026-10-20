//! Fetch rules: bound SPARQL imports, applied once per iteration like the other rules.
//!
//! A bound import `@import q(?x, ?y) from sparql { … } where b(?x), c(?y)`
//! compiles to a rule whose body atoms are the binding atoms and whose head is `q`.
//! Applying it queries the endpoint instead of joining.
//!
//! The evaluation is semi-naive.
//! Each application sends one family of requests per binding atom `j`, made of
//! the rows of atom `j` new since the last application,
//! the rows of the atoms before it as of the last application,
//! and the rows of the atoms after it as of now.
//! So every combination of binding rows is queried exactly once,
//! and over all applications the requests cover every binding the binding atoms derive.
//! A solution row is also fetched once, which keeps the nulls created for its blank nodes stable.
//! Each atom's rows are sent as one `VALUES` block per request, in pages,
//! and the solutions are deduplicated against the head table and stored as derived rows.
//!
//! The engine does not send the requests.
//! An application whose requests have no responses yet suspends as a [PendingFetch],
//! and completes once the responses have been passed to
//! [respond](crate::engine::Engine::respond).

use std::{collections::HashSet, ops::Range};

use nemo_io::{
    remote::Request,
    sparql::{self, ValuesBlock, spargebra::term::GroundTerm},
};
use nemo_physical::{
    diagnostics::Diagnostic,
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{ColumnOrder, Revision, TableId, TableManager},
    plan::{Marker, OperationPlan},
    storage::StorageValue,
    trie::{RowBuffer, Trie},
    value::ErrorCounts,
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

/// The query length beyond which a request is built again with a binding page half as wide.
///
/// It stays well under the size of a POST body endpoints accept.
const QUERY_CHAR_LIMIT: usize = 500_000;

/// The `VALUES` blocks one request carries.
///
/// If the endpoint finds the request too large, or cuts its response off, its widest block is
/// halved.
type ValuesBlocks = Vec<ValuesBlock>;

/// What one fetch application did beyond deriving rows.
#[derive(Debug, Default, Clone)]
pub(super) struct Fetched {
    /// Requests made, the halves of split ones included.
    pub(super) requests: u64,
    /// Solution rows received, before deduplication.
    pub(super) rows: u64,
    /// Rows in the responses that were not valid solutions.
    pub(super) malformed: u64,
    /// The solution rows a condition pushed into the import could not evaluate,
    /// counted per kind.
    ///
    /// They are dropped, as the rule would have dropped them.
    pub(super) errors: ErrorCounts,
    /// Requests that failed, under `on_error = warn`.
    pub(super) failed: u64,
    /// The binding combinations the failed requests carried, which are
    /// not queried again.
    pub(super) unasked: u64,
    /// Why the first failed request failed.
    pub(super) first_failure: Option<String>,
}

/// A fetch application waiting for the responses to its requests:
/// the rule, the step of the application,
/// and the requests, each with the `VALUES` blocks it carries.
#[derive(Debug)]
pub(in crate::engine) struct PendingFetch {
    /// The rule's index.
    pub(in crate::engine) rule: usize,
    /// The step of the application.
    pub(in crate::engine) step: u64,
    /// The requests.
    pub(in crate::engine) slots: RequestSlots<ValuesBlocks>,
    /// Requests made so far, the halves of split ones included.
    made: u64,
}

/// How a fetch application ended: applied, or suspended until its requests have responses.
#[derive(Debug)]
pub(super) enum FetchOutcome {
    /// Applied, with what it derived and what it did.
    Applied(Box<Applied>, Fetched),
    /// Suspended, waiting for the responses to these requests.
    Suspended(PendingFetch),
}

/// Applies a fetch rule at the given step.
///
/// The binding atoms' rows since the rule's last application are sent to the endpoint,
/// and the solutions become the head table's new rows,
/// without the rows it already stores.
///
/// The first time, the application builds its requests and suspends until they have responses.
/// Resumed with them, it splits the requests the endpoint found too large or cut off,
/// suspends again for the halves, and reads the other responses.
/// A failed request fails the application, or, under `on_error = warn`, is counted and skipped.
/// The import's conditions evaluate in the run's context.
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
            let requests = requests(rule, fetch, step, manager, dictionary, context)?;
            if requests.is_empty() {
                return Ok(FetchOutcome::Applied(Box::default(), Fetched::default()));
            }
            let mut pending = PendingFetch {
                rule: index,
                step,
                slots: RequestSlots::default(),
                made: 0,
            };
            for (request, blocks) in requests {
                pending.slots.push(request, blocks, ids);
                pending.made += 1;
            }
            tracing::debug!(
                endpoint = fetch.endpoint,
                requests = pending.made,
                step,
                "fetch application waits for its responses"
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

    // Deduplicate against the head: the batch is stored in the scratch
    // table at this step, and its difference to the stored rows is what
    // the application derived.
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
    /// Replaces every request the endpoint could not answer whole by two requests,
    /// each with one half of its widest `VALUES` block:
    /// one it found too large (413, 414),
    /// and one whose response it cut off partway, typically at its time limit.
    ///
    /// A request whose blocks are single rows keeps its response.
    fn split_too_large(&mut self, fetch: &CompiledFetch, ids: &mut u64) {
        let slots = std::mem::take(&mut self.slots.0);
        for slot in slots {
            let too_large = match &slot.response {
                Some(Err(failure)) => failure.too_large(),
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
/// There is one family of requests per binding atom,
/// with that atom reading its delta since the rule's last application,
/// the atoms before it reading their rows as of that application,
/// and the atoms after it reading their rows as of now.
/// A family with an empty input sends no request.
fn requests(
    rule: &CompiledRule,
    fetch: &CompiledFetch,
    step: u64,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Vec<(Request, ValuesBlocks)>, Diagnostic> {
    let last = Revision(rule.last_applied);
    let now = Revision(step);
    let count = rule.body.len();
    let mut requests = Vec::new();
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
        let rows = binding_rows(rule, fetch, &ranges, pivot, manager, dictionary, context)?;
        if rows.iter().any(Vec::is_empty) {
            continue;
        }
        requests.extend(paged_requests(fetch, &rows));
    }
    Ok(requests)
}

/// Returns the solution rows of every response,
/// read through the fetch's selection into one batch of the head's arity,
/// with the fetch's statistics.
///
/// A failed request, or a response that cannot be read, fails the application,
/// unless the import sets `on_error = warn`.
fn solutions(
    fetch: &CompiledFetch,
    arity: usize,
    pending: PendingFetch,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<(RowBuffer, Fetched), Diagnostic> {
    let written = fetch.query.arity();
    let mut buffer = RowBuffer::new(arity);
    let mut fetched = Fetched {
        requests: pending.made,
        ..Fetched::default()
    };
    let reader = sparql::SparqlReader::new(written, fetch.selection.clone());
    for slot in pending.slots.0 {
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

/// Returns the number of binding combinations a request queries:
/// the product of its blocks' row counts.
fn combinations(blocks: &ValuesBlocks) -> u64 {
    blocks.iter().fold(1u64, |product, block| {
        product.saturating_mul(block.rows.len() as u64)
    })
}

/// Returns the request querying the bindings of some `VALUES` blocks.
fn request_for(fetch: &CompiledFetch, blocks: &[ValuesBlock]) -> Request {
    request_with(fetch, &render(fetch, blocks))
}

/// Returns the fetch's query with some `VALUES` blocks.
fn render(fetch: &CompiledFetch, blocks: &[ValuesBlock]) -> String {
    // Translation only binds queries that are not opaque.
    fetch.query.with_values(blocks)
}

/// Returns the request sending a rendered query to the fetch's endpoint.
fn request_with(fetch: &CompiledFetch, text: &str) -> Request {
    let mut request = Request::sparql(&fetch.endpoint, text);
    request.headers.extend(fetch.headers.iter().cloned());
    request
}

/// Returns the rows of a batch not stored in the head table.
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

/// Returns one binding atom's rows within a revision range,
/// under the atom's own constraints (its constants and repetitions),
/// as a plan output in the atom's column order.
///
/// The endpoint joins the atoms, the plan does not.
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
/// as ground terms.
///
/// A row holding a null in a bound column is left out,
/// since a blank node cannot be sent as a SPARQL term.
/// The pivot atom reads its delta,
/// and a key of it that already existed before the delta is left out too,
/// since an earlier application queried it already,
/// and querying it again would create new nulls for the blank nodes in its solutions.
fn binding_rows(
    rule: &CompiledRule,
    fetch: &CompiledFetch,
    ranges: &[Range<Revision>],
    pivot: usize,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> Result<Vec<Vec<Vec<GroundTerm>>>, Diagnostic> {
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
            let mut terms = Vec::with_capacity(key.len());
            for value in key {
                let decoded = dictionary
                    .decode(value)
                    .expect("stored values decode against their dictionary");
                let Some(term) = sparql::SparqlQuery::ground_term(&decoded.to_owned()) else {
                    return;
                };
                terms.push(term);
            }
            rows.push(terms);
        });
        per_atom.push(rows);
    }
    Ok(per_atom)
}

/// Returns the requests querying one family of bindings:
/// every atom's rows split into pages, one request per combination of pages,
/// each page a `VALUES` block.
///
/// If a rendered query is too long, the widest page is halved and the requests are built again.
fn paged_requests(
    fetch: &CompiledFetch,
    rows: &[Vec<Vec<GroundTerm>>],
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
        let chunks: Vec<Vec<&[Vec<GroundTerm>]>> = rows
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
