use super::{Column,ColumnScan};
use std::fmt::Debug;

#[derive(Debug)]
struct RleElement<T> {
    value: T,
    length: usize,
    increment: i32,
}


//impl<T: std::ops::Add<i64, Output=T> + Debug + Copy> Index<usize> for RleElement<T> {
    //type Output = T;

    //fn index(&self, index: usize) -> &Self::Output {
        //&(self.value + i64::try_from(index).unwrap() * i64::from(self.increment))
    //}
//}

impl<T: std::ops::Add<i64, Output=T> + Debug + Copy> RleElement<T> {
    fn get(&self, index: usize) -> T {
        self.value + i64::try_from(index).unwrap() * i64::from(self.increment)
    }
}

/// Implementation of [`Column`] that allows the use of incremental run length encoding.
#[derive(Debug)]
pub struct RleColumn<T: std::ops::Add<i64, Output=T> + Ord + Copy> {
    elements: Vec<RleElement<T>>
}

impl<T: std::ops::Add<i64, Output=T> + Debug + Ord + Copy> RleColumn<T> {
    /// Constructs a new RleColumn from a vector of the suitable type.
    pub fn new(data: Vec<T>) -> RleColumn<T> {
        // TODO: actually use RLE
        RleColumn { elements: data.iter().map(|e| RleElement { value: *e, length: 1, increment: 0 }).collect() }
    }
}

impl<T: std::ops::Add<i64, Output=T> + Debug + Ord + Copy> Column<T> for RleColumn<T> {
    fn len(&self) -> usize {
        self.elements.iter().map(|e| e.length).sum()
    }

    fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn get(&self, index: usize) -> T {
        let mut target_index = index;
        let mut element_index = 0;
        while target_index > self.elements[element_index].length {
            target_index -= self.elements[element_index].length;
            element_index += 1;
        }
        self.elements[element_index].get(target_index)
    }

    fn iter<'a>(&'a self) -> Box<dyn ColumnScan<Item = T> + 'a> {
        Box::new(RleColumnScan::new(self))
    }
}

//impl<T: std::ops::Add<i64, Output=T> + Debug + Ord + Copy> Index<usize> for RleColumn<T> {
    //type Output = T;

    //fn index(&self, index: usize) -> &Self::Output {
         //&self.get(index)
    //}
//}

#[derive(Debug)]
struct RleColumnScan<'a, T: std::ops::Add<i64, Output=T> + Ord + Copy> {
    column: &'a RleColumn<T>,
    element_index: Option<usize>,
    increment_index: Option<usize>,
    current: Option<T>,
}

impl<'a, T: std::ops::Add<i64, Output=T> + Ord + Copy> RleColumnScan<'a, T> {
    pub fn new(column: &'a RleColumn<T>) -> RleColumnScan<'a, T> {
        RleColumnScan { 
            column, 
            element_index: None,
            increment_index: None,
            current: None,
        }
    }
}

impl<'a, T: std::ops::Add<i64, Output=T> + Ord + Copy> Iterator for RleColumnScan<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut element_index = self.element_index.unwrap_or_default();
        let mut increment_index = self.increment_index.map_or_else(Default::default, |i| i + 1);
        
        // only using while here for the case that e RleElement should have length 0, which isn't
        // intended anyway but better safe than sorry :)
        while element_index < self.column.elements.len() && increment_index >= self.column.elements[element_index].length {
            element_index += 1;
            increment_index = 0;
        }

        self.element_index = Some(element_index);
        self.increment_index = Some(increment_index);

        self.current = (element_index < self.column.elements.len()).then(||
            if increment_index == 0 {
                self.column.elements[element_index].value
            } else {
                // self.current should always contain a value here
                self.current.unwrap() + self.column.elements[element_index].increment.into()
            }
        );

        self.current
    }
}

impl<'a, T: std::ops::Add<i64, Output=T> + Debug + Ord + Copy> ColumnScan for RleColumnScan<'a, T> {

    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        // could we assume a sorted column here?
        // for now we don't
        for next in self {
            if next >= value {
                return Some(next)
            }
        }
        None
    }

    fn current(&mut self) -> Option<Self::Item> {
        self.current
    }
}

