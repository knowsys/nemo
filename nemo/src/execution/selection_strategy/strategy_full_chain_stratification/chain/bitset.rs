use std::fmt;

use super::trail::{Trail, TrailTarget, TrailEntry};

const WORD_BITS: usize = usize::BITS as usize;

#[derive(Clone, Debug)]
pub struct BitSet {
    /// machiene words to store the bits
    words: Box<[usize]>,
    /// number of used bits
    len: usize,
}

impl BitSet {
    fn write_word(&mut self, word: usize, value: usize, target: TrailTarget, trail: &mut Trail) {
        let old = self.words[word];

        if old != value {
            trail.entries.push(TrailEntry::Word { target, word, old });

            self.words[word] = value;
        }
    }

    pub fn restore_word(&mut self, word: usize, value: usize) {
        self.words[word] = value;
    }
}

impl BitSet {
    fn init(value: usize, len: usize) -> Self {
        let nwords = len.div_ceil(WORD_BITS);
        Self {
            words: vec![value; nwords].into_boxed_slice(),
            len,
        }
    }

    pub fn new(len: usize) -> Self {
        Self::init(0, len)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn filled(len: usize) -> Self {
        let mut bs = Self::init(usize::MAX, len);
        bs.clear_unused_bits();
        bs
    }

    fn clear_unused_bits(&mut self) {
        let rem = self.len % WORD_BITS;
        if rem > 0 {
            let mask = (1usize << rem) - 1;
            *self.words.last_mut().unwrap() &= mask;
        }
    }

    #[inline]
    fn index(bit: usize) -> (usize, usize) {
        (bit / WORD_BITS, bit % WORD_BITS)
    }

    #[inline]
    pub fn is_singleton(&self, value: usize) -> bool {
        self.count_ones() == 1 && self.contains(value)
    }

    #[inline]
    pub fn contains(&self, bit: usize) -> bool {
        let (w, b) = Self::index(bit);
        (self.words[w] & (1usize << b)) != 0
    }

    #[inline]
    pub fn insert(&mut self, bit: usize) {
        let (w, b) = Self::index(bit);
        self.words[w] |= 1usize << b;
    }

    #[inline]
    pub fn remove(&mut self, bit: usize) {
        let (w, b) = Self::index(bit);
        self.words[w] &= !(1usize << b);
    }

    #[inline]
    pub fn remove_trail(&mut self, bit: usize, target: TrailTarget, trail: &mut Trail) {
        let (w, b) = Self::index(bit);

        let value = self.words[w] & !(1usize << b);

        self.write_word(w, value, target, trail);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.words.fill(0);
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    #[inline]
    pub fn intersects(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(&other.words)
            .any(|(&a, &b)| (a & b) != 0)
    }

    #[inline]
    pub fn intersect_with_trail(
        &mut self,
        other: &Self,
        target: TrailTarget,
        trail: &mut Trail,
    ) -> bool {
        let mut changed = false;

        for i in 0..self.words.len() {
            let value = self.words[i] & other.words[i];

            changed |= self.words[i] != value;

            self.write_word(i, value, target, trail);
        }

        changed
    }

    #[inline]
    pub fn union_with(&mut self, other: &Self) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
    }

    #[inline]
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn first_set(&self) -> Option<usize> {
        for (i, &word) in self.words.iter().enumerate() {
            if word != 0 {
                let bit = word.trailing_zeros() as usize;
                return Some(i * WORD_BITS + bit);
            }
        }
        None
    }
}

pub struct Ones<'a> {
    words: std::iter::Copied<std::slice::Iter<'a, usize>>,
    current: usize,
    base: usize,
}

impl BitSet {
    pub fn iter_ones(&self) -> Ones<'_> {
        let mut words = self.words.iter().copied();
        let current = words.next().unwrap_or(0);
        Ones {
            words,
            current,
            base: 0,
        }
    }
}

impl<'a> Iterator for Ones<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                return Some(self.base + bit);
            }

            self.current = self.words.next()?;
            self.base += WORD_BITS;
        }
    }
}

pub struct SnapshotOnes {
    words: Box<[usize]>,
    current: usize,
    word_index: usize,
}

impl BitSet {
    pub fn iter_ones_snapshot(&self) -> SnapshotOnes {
        let words = self.words.clone();

        let current = if words.is_empty() { 0 } else { words[0] };

        SnapshotOnes {
            words,
            current,
            word_index: 0,
        }
    }
}

impl Iterator for SnapshotOnes {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current != 0 {
                let bit = self.current.trailing_zeros() as usize;
                self.current &= self.current - 1;
                return Some(self.word_index * WORD_BITS + bit);
            }

            self.word_index += 1;

            if self.word_index >= self.words.len() {
                return None;
            }

            self.current = self.words[self.word_index];
        }
    }
}

impl BitSet {
    /// remove if pred returns false
    pub fn remove_if<F>(&mut self, mut pred: F) -> bool
    where
        F: FnMut(usize) -> bool,
    {
        let mut changed = false;

        for (word_idx, word) in self.words.iter_mut().enumerate() {
            let mut keep = *word;
            let mut bits = *word;

            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;

                let value = word_idx * WORD_BITS + bit;

                if pred(value) {
                    keep &= !(1usize << bit);
                    changed = true;
                }
            }

            *word = keep;
        }

        changed
    }

    pub fn remove_if_trail<F>(
        &mut self,
        mut pred: F,
        target: TrailTarget,
        trail: &mut Trail,
    ) -> bool
    where
        F: FnMut(usize) -> bool,
    {
        let mut changed = false;

        for word_index in 0..self.words.len() {
            let old = self.words[word_index];
            let mut new = old;

            let mut bits = old;

            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;

                let value = word_index * WORD_BITS + bit;

                if pred(value) {
                    new &= !(1usize << bit);
                }
            }

            if new != old {
                self.write_word(word_index, new, target, trail);
                changed = true;
            }
        }

        changed
    }
}

impl fmt::Display for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rem = self.len % WORD_BITS;
        let (it1, it2) = if rem > 0 {
            let mask = (1usize << rem) - 1;
            let last = *self.words.last().unwrap() & mask;
            (self.words[..self.words.len() - 1].iter(), Some((last, rem)))
        } else {
            (self.words.iter(), None)
        };
        write!(
            f,
            "{}",
            it1.copied()
                .map(|w| (w, WORD_BITS))
                .chain(it2.into_iter())
                .map(|(w, bits)| format!("{:0width$b}", w, width = bits))
                .collect::<String>()
        )
    }
}
