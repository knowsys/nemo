use rand::Rng;

use super::hypergraph::Hypergraph;
use super::solver::Solver;
use super::core_solver::CoreSolver;
use super::domain::Domains;

type Graph = Vec<Vec<Vec<usize>>>;

fn gac(h: &Hypergraph, g: &Hypergraph) -> Option<Domains> {
    let mut solver = Solver::from_hypergraphs(h, g);
    match solver.propagate_all() {
        Ok(()) => Some(Domains(solver.domains)),
        Err(()) => None,
    }
}

fn hom(h: &Hypergraph, g: &Hypergraph) -> Option<Vec<usize>> {
    let mut solver = Solver::from_hypergraphs(h, g);
    match solver.solve() {
        true => Some(solver.solution()),
        false => None,
    }
}

#[derive(Clone)]
struct SolverState {
    domains: Box<[Domain]>,
    alive: Box<[BitSet]>,
}

impl<'a> Solver<'a> {
    fn save_state(&self) -> SolverState {
        SolverState {
            domains: self.domains.clone(),
            alive: self.constraints.iter().map(|c| c.alive.clone()).collect(),
        }
    }

    fn restore_state(&mut self, state: SolverState) {
        self.domains = state.domains;

        for (constraint, alive) in self.constraints.iter_mut().zip(state.alive) {
            constraint.alive = alive;
        }

        self.queue.clear();
        self.queued.fill(false);
    }

    pub fn solve_full_restore(&mut self) -> bool {
        if self.propagate_all().is_err() {
            return false;
        }

        if self.solved() {
            return true;
        }

        let var = self
            .choose_variable()
            .expect("unsolved graph should have variables to choose");

        for value in self.domains[var]
            .as_bitset_ref()
            .expect("chosen variable must be unassigned")
            .iter_ones_snapshot()
        {
            let state = self.save_state();

            self.assign(var, value);

            if self.solve_full_restore() {
                return true;
            }

            self.restore_state(state);
        }

        false
    }
}

fn hom_full_restore(h: &Hypergraph, g: &Hypergraph) -> Option<Vec<usize>> {
    let mut solver = Solver::from_hypergraphs(h, g);
    match solver.solve_full_restore() {
        true => Some(solver.solution()),
        false => None,
    }
}

fn is_homomorphism(h: &Graph, g: &Graph, map: &[usize]) -> bool {
    h.iter().zip(g.iter()).all(|(h_edges, g_edges)| {
        h_edges.iter().all(|edge| {
            g_edges.iter().any(|candidate| {
                edge.iter()
                    .zip(candidate.iter())
                    .all(|(&v, &mapped)| map[v] == mapped)
            })
        })
    })
}

fn vertex_count(graph: &Graph) -> usize {
    graph
        .iter()
        .flatten()
        .flatten()
        .copied()
        .max()
        .map(|v| v + 1)
        .unwrap_or(0)
}

fn brute_force(h: &Graph, g: &Graph) -> Option<Vec<usize>> {
    let n = vertex_count(h);
    let m = vertex_count(g);

    let mut map = vec![0; n];

    fn dfs(i: usize, n: usize, m: usize, h: &Graph, g: &Graph, map: &mut Vec<usize>) -> bool {
        if i == n {
            return is_homomorphism(h, g, map);
        }

        for value in 0..m {
            map[i] = value;

            if dfs(i + 1, n, m, h, g, map) {
                return true;
            }
        }

        false
    }

    if dfs(0, n, m, h, g, &mut map) {
        Some(map)
    } else {
        None
    }
}

// dense vertex usage

fn random_graph(rng: &mut impl Rng, vertices: usize, arities: &[usize], p: f64) -> Graph {
    let colors = arities.len();
    let mut graph = vec![Vec::new(); colors];

    // Add random tuples.
    for color in 0..colors {
        let arity = arities[color];
        let tuples = vertices.pow(arity as u32);

        for index in 0..tuples {
            if rng.random_bool(p) {
                let mut tuple = Vec::with_capacity(arity);

                let mut x = index;

                for _ in 0..arity {
                    tuple.push(x % vertices);
                    x /= vertices;
                }

                graph[color].push(tuple);
            }
        }

        // Respect the application invariant: no empty relations.
        if graph[color].is_empty() {
            graph[color].push((0..arity).map(|_| rng.random_range(0..vertices)).collect());
        }
    }

    // Ensure dense vertex usage by inserting missing vertices at random positions.
    let mut used = vec![false; vertices];

    for edges in &graph {
        for edge in edges {
            for &v in edge {
                used[v] = true;
            }
        }
    }

    for v in 0..vertices {
        if !used[v] {
            let color = rng.random_range(0..colors);
            let arity = arities[color];
            let position = rng.random_range(0..arity);

            let mut tuple = (0..arity)
                .map(|_| rng.random_range(0..vertices))
                .collect::<Vec<_>>();

            tuple[position] = v;

            graph[color].push(tuple);
        }
    }

    graph
}

impl From<Graph> for Hypergraph {
    fn from(g: Graph) -> Self {
        let variable_count = g
            .iter()
            .flatten()
            .flatten()
            .copied()
            .max()
            .map(|x| x + 1)
            .unwrap_or(0);

        let tuples: Vec<Tuples> = g
            .into_iter()
            .map(|t| {
                assert!(!t.is_empty());
                let arity = t[0].len();
                let data = t.into_iter().flat_map(|tt| tt.into_iter()).collect();
                Tuples { arity, data }
            })
            .collect();

        let mut incidence = vec![Vec::new(); variable_count];

        for (edge_id, vars) in tuples.iter().enumerate().flat_map(|(color, t)| {
            t.iter()
                .enumerate()
                .map(move |(idx, tt)| (EdgeId { color, idx }, tt))
        }) {
            for &v in vars {
                incidence[v].push(edge_id.clone());
            }
        }

        Self::new(tuples, incidence)
    }
}

#[test]
fn random_compare_homomorphism_against_bruteforce() {
    let mut rng = rand::rng();

    let cases = [
        // tiny exhaustive cases
        // iterations, vertices, colors, max_arity, density, brute
        (2000, 2, 1, 2, 0.2, true),
        (2000, 3, 1, 2, 0.5, true),
        (2000, 4, 2, 2, 0.3, true),
        (1000, 4, 3, 2, 0.8, true),
        (1000, 4, 2, 3, 0.3, true),
        (1000, 5, 2, 2, 0.5, true),
        (1000, 5, 2, 2, 0.1, true),
        // different arities per color
        (1000, 4, 3, 3, 0.3, true),
        (1000, 5, 3, 4, 0.2, true),
        // sparse graphs (often expose propagation issues)
        (500, 6, 2, 3, 0.1, false),
        // dense graphs (many surviving tuples)
        (500, 6, 3, 3, 0.8, false),
        // larger search spaces
        (100, 8, 3, 3, 0.3, false),
        (100, 10, 4, 4, 0.2, false),
    ];
    //let cases = vec![];

    for &(iterations, vertices, colors, max_arity, p, use_brute) in &cases {
        println!(
            "iterations={iterations}, vertices={vertices}, colors={colors}, max_arity={max_arity}, p={p}, use_brute={use_brute}"
        );
        for _ in 0..iterations {
            let arities: Vec<_> = (0..colors)
                .map(|_| rng.random_range(1..=max_arity))
                .collect();
            let h = random_graph(&mut rng, vertices, &arities, p);
            let g = random_graph(&mut rng, vertices, &arities, p);
            //println!("H = {h:?}\nG = {g:?}");

            let g_hyper = g.clone().into();
            let h_hyper = h.clone().into();
            let full = hom_full_restore(&h_hyper, &g_hyper);
            let trail = hom(&h_hyper, &g_hyper);

            for (nom, res) in [("full", &full), ("trail", &trail)] {
                if let Some(mapping) = res {
                    assert!(
                        is_homomorphism(&h, &g, &mapping),
                        "H = {h:?}\nG = {g:?}\nf = {mapping:?}\nFailure for '{nom}': f is no homomorphism from H to G"
                    );
                }
            }

            if full.is_none() && use_brute {
                let brute = brute_force(&h, &g);

                assert!(
                    brute.is_none(),
                    "solver missed a homomorphism from H to G\nH = {h:?}\nG = {g:?}",
                );
            }

            assert_eq!(
                full.is_some(),
                trail.is_some(),
                "'full' shows that H is{} homomorphic to G\nH = {h:?}\nG = {g:?}",
                if full.is_some() { "" } else { " not" }
            );
        }
    }
}

fn is_non_injective(map: &[usize]) -> bool {
    map.iter().enumerate().any(|(i, &j)| i != j)
}

fn is_idempotent(map: &[usize]) -> bool {
    for x in 0..map.len() {
        if map[map[x]] != map[x] {
            return false;
        }
    }
    true
}

fn is_retraction(g: &Graph, map: &[usize]) -> bool {
    is_homomorphism(g, g, map) && is_idempotent(map)
}

fn is_proper_retraction(g: &Graph, map: &[usize]) -> bool {
    is_retraction(g, map) && is_non_injective(map)
}

fn brute_force_proper_retraction(g: &Graph) -> Option<Vec<usize>> {
    let n = vertex_count(g);

    let mut map = vec![0; n];

    fn dfs(i: usize, n: usize, g: &Graph, map: &mut Vec<usize>) -> bool {
        if i == n {
            return is_proper_retraction(g, map);
        }

        for value in 0..n {
            map[i] = value;

            if dfs(i + 1, n, g, map) {
                return true;
            }
        }

        false
    }

    if dfs(0, n, g, &mut map) {
        Some(map)
    } else {
        None
    }
}

#[test]
fn random_compare_retraction_against_bruteforce() {
    let mut rng = rand::rng();

    let cases = [
        // iterations, vertices, colors, max_arity, density
        (2000, 2, 1, 2, 0.2),
        (2000, 3, 1, 2, 0.5),
        (2000, 4, 2, 2, 0.3),
        (1000, 4, 3, 2, 0.8),
        (1000, 4, 2, 3, 0.3),
        (1000, 5, 2, 2, 0.5),
        (1000, 5, 2, 2, 0.1),
        (50, 7, 3, 3, 0.1),
    ];
    //let cases = vec![];

    for &(iterations, vertices, colors, max_arity, p) in &cases {
        println!(
            "iterations={iterations}, vertices={vertices}, colors={colors}, max_arity={max_arity}, p={p}"
        );

        for _ in 0..iterations {
            let arities: Vec<_> = (0..colors)
                .map(|_| rng.random_range(1..=max_arity))
                .collect();

            let g = random_graph(&mut rng, vertices, &arities, p);

            let brute = brute_force_proper_retraction(&g);

            let g_hyper = g.clone().into();
            let mut solver = CoreSolver::from_hypergraph(&g_hyper);

            assert!(
                solver.solve_retraction(),
                "Identity retraction should always exist\nG={g:?}"
            );

            let map = solver.solution();

            assert!(
                is_retraction(&g, &map),
                "Solver returned invalid retraction\nG={g:?}\nmap={map:?}"
            );

            let solver_found_collapse = is_non_injective(&map);

            assert_eq!(
                brute.is_some(),
                solver_found_collapse,
                "Mismatch\nG={g:?}\nsolver map={map:?}\nbrute={brute:?}"
            );
        }
    }
}

fn core(g: &Hypergraph) -> (Graph, Option<Vec<usize>>) {
    let mut solver = CoreSolver::from_hypergraph(&g);
    let mut retraction = None;
    loop {
        assert!(
            solver.solve_retraction(),
            "should always have identity retraction {g}"
        );

        let collapsed_nodes = solver.collapsed_nodes();
        if collapsed_nodes.is_empty() {
            // normally, would use Hypergraph::remove_vertices, which "densifies", but for testing we want to keep vertex ids stable to test homomorphism first
            return (
                g.tuples()
                    .iter()
                    .map(|tuples| {
                        tuples
                            .iter()
                            .filter(|tuple| {
                                tuple
                                    .into_iter()
                                    .all(|v| solver.is_alive(*v))
                            })
                            .map(|t| t.to_vec())
                            .collect()
                    })
                    .collect(),
                retraction,
            );
        }

        let r_step = solver.solution();
        retraction = match retraction {
            Some(r) => Some(r.iter().map(|j| r_step[*j]).collect()),
            None => Some(r_step),
        };

        solver.apply_retraction(&collapsed_nodes);
    }
}

fn densify(g: &Graph) -> (Graph, Vec<usize>) {
    let n = vertex_count(g);

    let mut rename = vec![usize::MAX; n];
    let mut next = 0;

    for v in g.iter().flatten().flatten().copied() {
        if rename[v] == usize::MAX {
            rename[v] = next;
            next += 1;
        }
    }

    let dense = g
        .iter()
        .map(|edges| {
            edges
                .iter()
                .map(|edge| edge.iter().map(|&v| rename[v]).collect())
                .collect()
        })
        .collect();

    (dense, rename)
}

#[test]
fn random_compare_core_against_bruteforce() {
    let mut rng = rand::rng();

    let cases = [
        // iterations, vertices, colors, max_arity, density
        (1000, 3, 1, 2, 0.3),
        (1000, 4, 2, 2, 0.3),
        (1000, 5, 2, 3, 0.2),
        (500, 6, 3, 3, 0.4),
    ];
    //let cases = vec![];

    for &(iterations, vertices, colors, max_arity, p) in &cases {
        for _ in 0..iterations {
            let arities: Vec<_> = (0..colors)
                .map(|_| rng.random_range(1..=max_arity))
                .collect();

            let g = random_graph(&mut rng, vertices, &arities, p);
            //let g = vec![vec![vec![2, 0], vec![0, 2], vec![1, 2]]];

            let (core, map) = core(&g.clone().into());

            let (dense_core, rename) = densify(&core);
            assert!(is_homomorphism(&core, &dense_core, &rename));

            // The result must itself be a core.
            assert!(
                brute_force_proper_retraction(&dense_core).is_none(),
                "Returned structure is not a core\n\
                 G={g:?}\n\
                 Core={core:?}"
            );

            // If there was a retraction map, verify it.
            if let Some(map) = map {
                assert!(
                    is_homomorphism(&g, &core, &map),
                    "Returned map is not a homomorphism\n\
                     G={g:?}\n\
                     Core={core:?}\n\
                     map={map:?}"
                );
                assert!(is_idempotent(&map));
                assert!(is_non_injective(&map))
            }

            assert!(vertex_count(&dense_core) <= vertex_count(&g));
        }
    }
}
