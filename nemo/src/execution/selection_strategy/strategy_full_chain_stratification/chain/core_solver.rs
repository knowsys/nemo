use super::bitset::BitSet;
use super::solver::Solver;
use super::hypergraph::Hypergraph;
use super::domain::Domain;
use super::trail::TrailTarget;

// homomorphism from $G$ to $H$: $e \in G$ implies $f(e) \in H$
// endomorphism: homomorphism from $G$ to $G$
// strong: $\neg e \in G$ implies $\neg f(e) \in G$, or equivalently: $e \in G$ implies $f^{-1}(e) \subseteq G$ ("f reflects all relations")
// "weak core": every endomorphism is an embedding (strong, injective)
// "strong core": every endomorphism is an automorphism (strong, injective, surjective)
// on a finite set A, every mapping $A\to A$ is injective iff it is surjective, so "weak" and "strong" core coincide on finite domains
// "super-weak core": every endomorphism is injective
// on finite relational structures, "super-weak core" just means that every endomorphism is a permutation of the finite set of vertices,
// which has an "order" $m\in\mathbb{N}$, s.t. $f^m = id, so $f(e)\in G => f^2(e)\in G => ... => f^m(e) = e \in G$, so "strength" follows,
// i.e. "super-weak" and "weak" also coincide on finite domains
// --> so it is legitimate
// retraction: idempotent endomorphism, i.e. f(f(e)) = f(e); "proper" if non-injective / the result of applying a retraction is called "retract"
// If a finite relational structure has a non-injective endomorphism, then it has a non-injective idempotent endomorphism,
// because there is m such that A \supseteq f(A)\supseteq f^2(A) \supseteq ... \supseteq f^m(A) = f^{m+1}(A) =: B,
// so g = f\restriction_B is permutation and thus has finite order k such that g^k=id_B, therefore r = f^{mk} is a retraction and it is proper since f was injective
// ...so we can just search for proper retracts

pub struct CoreSolver<'a> {
    solver: Solver<'a>,
    pub alive_vertices: BitSet,
    alive_constraints: BitSet,
}

impl<'a> CoreSolver<'a> {
    pub fn from_hypergraph(g: &'a Hypergraph) -> Self {
        let solver = Solver::from_hypergraphs(g, g);
        Self {
            alive_vertices: BitSet::filled(solver.domains.len()),
            alive_constraints: BitSet::filled(solver.constraints.len()),
            solver,
        }
    }

    fn assign_retraction(&mut self, var: usize, value: usize) -> Result<(), ()> {
        // Assign var -> value.
        self.solver.assign(var, value);

        // Force value to be fixed.
        match &self.solver.domains[value] {
            Domain::Unassigned(bitset) => {
                if !bitset.is_singleton(value) {
                    self.solver.assign(value, value);
                }
            }
            Domain::Assigned(assigned_val) => {
                if *assigned_val != value {
                    return Err(());
                }
            }
        }

        // Remove var from every image domain.
        for other in 0..self.solver.domains.len() {
            match &mut self.solver.domains[other] {
                Domain::Unassigned(bitset) => {
                    if bitset.contains(var) {
                        bitset.remove_trail(
                            var,
                            TrailTarget::Domain(other),
                            &mut self.solver.trail,
                        );

                        for &cid in &self.solver.incident[other] {
                            Solver::enqueue(&mut self.solver.queued, &mut self.solver.queue, cid);
                        }
                    }
                }
                Domain::Assigned(value) => {
                    if *value == var {
                        return Err(());
                    }
                }
            }
        }

        return Ok(());
    }

    pub fn solution(&self) -> Vec<usize> {
        self.solver.solution()
    }

    pub fn collapsed_nodes(&self) -> Vec<(usize, usize)> {
        self.solver
            .solution()
            .into_iter()
            .enumerate()
            .filter(|(i, j)| self.alive_vertices.contains(*i) && i != j)
            .collect()
    }

    fn choose_variable_retraction(&self) -> Option<usize> {
        self.solver.choose_variable_aux(
            self.solver
                .domains
                .iter()
                .enumerate()
                .filter(|(v, _)| self.alive_vertices.contains(*v)),
        )
    }

    pub fn solve_retraction(&mut self) -> bool {
        if self.solver.propagate_all().is_err() {
            return false;
        }

        if self.solver.solved() {
            return true;
        }

        let var = self
            .choose_variable_retraction()
            .expect("unsolved graph should have variables to choose");

        let mut defer_inj = false;

        for value in self.solver.domains[var]
            .as_bitset_ref()
            .expect("chosen variable must be unassigned")
            .iter_ones_snapshot()
        {
            if value == var {
                defer_inj = true;
                continue;
            }
            self.solver.trail.push_level();

            if self.assign_retraction(var, value).is_ok() {
                if self.solve_retraction() {
                    return true;
                }
            }

            self.solver.undo();
        }
        // prefer non-injective mappings
        if defer_inj {
            self.solver.trail.push_level();

            self.solver.assign(var, var);

            if self.solve_retraction() {
                return true;
            }

            self.solver.undo();
        }

        false
    }

    pub fn apply_retraction(&mut self, collapsed_nodes: &[(usize, usize)]) {
        for &(v, _) in collapsed_nodes {
            self.solver.domains[v] = Domain::Assigned(v);
            self.alive_vertices.remove(v);
        }
        for v in self.alive_vertices.iter_ones() {
            self.solver.domains[v] = Domain::Unassigned(self.alive_vertices.clone());
        }
        self.alive_constraints.remove_if(|cid| {
            self.solver.constraints[cid]
                .vars
                .iter()
                .any(|v| !self.alive_vertices.contains(*v))
        });
        for (cid, constraint) in self.solver.constraints.iter_mut().enumerate() {
            if self.alive_constraints.contains(cid) {
                let count = self.solver.relations[constraint.relation].tuple_count;
                constraint.alive = BitSet::filled(count);
            }
        }
        for (var, incident) in self.solver.incident.iter_mut().enumerate() {
            if self.alive_vertices.contains(var) {
                incident.retain(|other| self.alive_constraints.contains(*other));
            }
        }
        self.solver.queue.clear();
        self.solver.queue.extend(self.alive_constraints.iter_ones());
        self.solver.queued.fill(true);
        self.solver.trail.entries.clear();
        self.solver.trail.levels.clear();
    }

    pub fn is_alive(&self, vertex: usize) -> bool {
        self.alive_vertices.contains(vertex)
    }
}
