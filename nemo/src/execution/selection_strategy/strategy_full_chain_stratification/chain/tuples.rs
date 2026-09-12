#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuples {
    pub(super) arity: usize,
    /// flat tuple storage
    /// lexicographically sorted; set-semantics
    pub(super) data: Vec<usize>,
}

impl Tuples {
    pub fn new(arity: usize) -> Self {
        debug_assert!(arity > 0);
        Self {
            arity,
            data: Vec::new(),
        }
    }
    /// Build from a sequence of equal-arity rows.
    pub(crate) fn from_rows(arity: usize, rows: impl IntoIterator<Item = Vec<usize>>) -> Self {
        let mut data = Vec::new();
        for row in rows {
            debug_assert_eq!(row.len(), arity);
            data.extend(row);
        }
        Self { arity, data }
    }

    pub fn iter<'a>(&'a self) -> TuplesIter<'a> {
        TuplesIter { rel: self, i: 0 }
    }

    pub fn len(&self) -> usize {
        debug_assert!(self.data.len() % self.arity == 0);
        self.data.len() / self.arity
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(usize, &[usize]) -> bool,
    {
        let arity = self.arity;
        let len = self.data.len() / arity;

        let mut write = 0;

        for read in 0..len {
            let start = read * arity;
            let end = start + arity;

            if f(read, &self.data[start..end]) {
                if write != read {
                    self.data.copy_within(start..end, write * arity);
                }
                write += 1;
            }
        }

        self.data.truncate(write * arity);
    }
}

impl std::ops::Index<usize> for Tuples {
    type Output = [usize];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * self.arity;
        &self.data[start..start + self.arity]
    }
}

impl std::ops::IndexMut<usize> for Tuples {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        let start = index * self.arity;
        &mut self.data[start..start + self.arity]
    }
}

pub(crate) struct TuplesIter<'a> {
    rel: &'a Tuples,
    i: usize,
}

impl<'a> Iterator for TuplesIter<'a> {
    type Item = &'a [usize];

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.rel.data.len() {
            let old_i = self.i;
            self.i += self.rel.arity;
            Some(&self.rel.data[old_i..self.i])
        } else {
            None
        }
    }
}
