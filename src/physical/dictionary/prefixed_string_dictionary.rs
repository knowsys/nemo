use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::Display,
    hash::{Hash, Hasher},
    rc::{Rc, Weak},
};

use super::Dictionary;

/// Represents a node, which is either a [TrieNode::Root], or some non-special [TrieNode::Node]
enum TrieNode {
    Root {
        prefix: String,
        children: Vec<Rc<RefCell<TrieNode>>>,
    },
    Node {
        prefix: String,
        children: Vec<Rc<RefCell<TrieNode>>>,
        parent: Weak<RefCell<TrieNode>>,
    },
}

impl std::fmt::Debug for TrieNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrieNode::Root { prefix, children } => f
                .debug_struct("TrieNode::Root")
                .field("prefix", prefix)
                .field("children", children)
                .finish(),
            TrieNode::Node {
                prefix,
                children,
                parent,
            } => f
                .debug_struct("TrieNode::Node")
                .field("prefix", prefix)
                .field("children", children)
                .field(
                    "parent(prefix)",
                    &parent
                        .upgrade()
                        .expect("should be valid")
                        .as_ref()
                        .borrow()
                        .prefix(),
                )
                .finish(),
        }
    }
}

impl Default for TrieNode {
    fn default() -> Self {
        Self::Root {
            prefix: "".to_string(),
            children: Vec::new(),
        }
    }
}

impl Hash for TrieNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prefix().hash(state);
        match self {
            TrieNode::Root {
                prefix: _,
                children: _,
            } => {
                "".hash(state);
            }
            TrieNode::Node {
                prefix: _,
                children: _,
                parent,
            } => {
                parent
                    .upgrade()
                    .expect("should be valid")
                    .as_ref()
                    .borrow()
                    .hash(state);
            }
        }
    }
}

impl PartialEq for TrieNode {
    fn eq(&self, other: &Self) -> bool {
        match self {
            TrieNode::Root {
                prefix: left,
                children: _,
            } => match other {
                TrieNode::Root {
                    prefix: right,
                    children: _,
                } => left.eq(right),
                TrieNode::Node {
                    prefix: _,
                    children: _,
                    parent: _,
                } => false,
            },
            TrieNode::Node {
                prefix: left,
                children: _,
                parent: parent_left,
            } => match other {
                TrieNode::Root {
                    prefix: _,
                    children: _,
                } => false,
                TrieNode::Node {
                    prefix: right,
                    children: _,
                    parent: parent_right,
                } => {
                    left.eq(right)
                        && parent_left
                            .upgrade()
                            .expect("should be valid")
                            .as_ref()
                            .borrow()
                            .eq(&*parent_right
                                .upgrade()
                                .expect("should be valid")
                                .as_ref()
                                .borrow())
                }
            },
        }
    }
}

impl Eq for TrieNode {}

impl Display for TrieNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrieNode::Root {
                prefix: _,
                children: _,
            } => {}
            TrieNode::Node {
                prefix: _,
                children: _,
                parent,
            } => {
                write!(
                    f,
                    "{}",
                    parent.upgrade().expect("should be valid").as_ref().borrow()
                )?;
            }
        }

        write!(f, "{}", self.prefix())
    }
}

impl TrieNode {
    /// create a [TrieNode] with a given `parent` as well as a `prefix` [String]
    fn create_node(parent: Rc<RefCell<TrieNode>>, prefix: String) -> Self {
        Self::Node {
            prefix,
            children: Vec::new(),
            parent: Rc::downgrade(&parent),
        }
    }

    /// add another child to the list of children of this node
    /// Note that it does not check whether some child already exists
    fn add_node(&mut self, child: Rc<RefCell<TrieNode>>) {
        match self {
            Self::Node {
                children,
                prefix: _,
                parent: _,
            } => children,
            Self::Root {
                children,
                prefix: _,
            } => children,
        }
        .push(Rc::clone(&child));
    }

    /// Returns the children of the given [TrieNode]
    fn children(&self) -> Option<&Vec<Rc<RefCell<TrieNode>>>> {
        match self {
            Self::Node {
                children,
                prefix: _,
                parent: _,
            } => Some(children),
            Self::Root {
                children,
                prefix: _,
            } => Some(children),
        }
    }

    /// Provides the prefix of the given [TrieNode]
    fn prefix(&self) -> &str {
        match self {
            Self::Node {
                children: _,
                prefix,
                parent: _,
            } => prefix,
            Self::Root {
                children: _,
                prefix,
            } => prefix,
        }
    }

    /// Given a prefix, checks whether the prefix occurs in the children. Returns that child if that is the case.
    fn matching_child(&self, search_value: &str) -> Option<Rc<RefCell<TrieNode>>> {
        if let Some(children) = self.children() {
            for child in children {
                if child.as_ref().borrow().prefix().eq(search_value) {
                    return Some(Rc::clone(child));
                }
            }
        }
        None
    }

    /// Given a list of prefixes `search_list` and a `node`, searches for a matching path down the sub-tree with respect to the given list.
    /// It will return the last matching node, together with the remainder of the (not-matched) prefixes
    fn find_last_match<'a>(
        node: Rc<RefCell<TrieNode>>,
        search_list: &'a [&'a str],
    ) -> (Rc<RefCell<TrieNode>>, &'a [&'a str]) {
        let mut cur_node = node;
        for (pos, element) in search_list.iter().enumerate() {
            let mut helper = Rc::clone(&cur_node);
            if let Some(child) = cur_node.as_ref().borrow().matching_child(element) {
                helper = Rc::clone(&child);
            } else {
                return (helper, &search_list[pos..]);
            }
            cur_node = Rc::clone(&helper);
        }
        (cur_node, &[])
    }
}

/// Newtype to represent a pair of a [TreeNode] (i.e. a path in the Tree) and a [String]
#[derive(Clone, Debug)]
struct TrieNodeStringPair(Rc<RefCell<TrieNode>>, Rc<String>);

impl Hash for TrieNodeStringPair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().borrow().hash(state);
        self.1.hash(state);
    }
}

impl PartialEq for TrieNodeStringPair {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1) && self.0.eq(&other.0)
    }
}

impl Eq for TrieNodeStringPair {}

/// The [PrefixedStringDictionary] allows to store (and own) a couple of prefixed [String]s.
/// Prefixes will be stored in a Triestructure and each chain of prefixes is therefore only stored once.
#[derive(Clone, Debug)]
pub struct PrefixedStringDictionary {
    ordering: Vec<TrieNodeStringPair>,
    mapping: HashMap<TrieNodeStringPair, usize>,
    store: Rc<RefCell<TrieNode>>,
}

impl PrefixedStringDictionary {}

impl Default for PrefixedStringDictionary {
    fn default() -> Self {
        Self {
            ordering: Vec::new(),
            mapping: HashMap::new(),
            store: Rc::new(RefCell::new(TrieNode::default())),
        }
    }
}

impl Dictionary for PrefixedStringDictionary {
    fn add(&mut self, entry: String) -> usize {
        log::trace!("add {entry:?} to {self:?}");
        let prefixes: Vec<&str> = Prefixer::new(entry.as_str()).collect();
        log::trace!("prefixes: {prefixes:?}");
        let (real_prefixes, real_entry) = prefixes.split_at(prefixes.len() - 1);
        log::trace!("reals: {real_prefixes:?}, {real_entry:?}");
        let (mut cur_node, remaining_prefixes) =
            TrieNode::find_last_match(self.store.to_owned(), real_prefixes);
        log::trace!("cur_node: {cur_node:?}, remaining: {remaining_prefixes:?}");
        for element in remaining_prefixes {
            let new_node = Rc::new(RefCell::new(TrieNode::create_node(
                Rc::clone(&cur_node),
                element.to_string(),
            )));
            log::trace!("{element:?} ({remaining_prefixes:?}): new_node: {new_node:?}");
            cur_node
                .as_ref()
                .borrow_mut()
                .add_node(Rc::clone(&new_node));
            cur_node = Rc::clone(&new_node);
            log::trace!("{element:?} ({remaining_prefixes:?}): cur_node: {cur_node:?}");
        }
        let entry_string = Rc::new(real_entry[0].to_string());
        log::trace!("entry_string: {entry_string:?}");
        log::trace!("mapping: {:?}", self.mapping);
        log::trace!(
            "pair: {:?}",
            TrieNodeStringPair(Rc::clone(&cur_node), Rc::clone(&entry_string))
        );
        log::trace!(
            "entry: {:?}",
            self.mapping.entry(TrieNodeStringPair(
                Rc::clone(&cur_node),
                Rc::clone(&entry_string),
            )),
        );
        *self
            .mapping
            .entry(TrieNodeStringPair(
                Rc::clone(&cur_node),
                Rc::clone(&entry_string),
            ))
            .or_insert_with(|| {
                let value = self.ordering.len();
                self.ordering.push(TrieNodeStringPair(
                    Rc::clone(&cur_node),
                    Rc::clone(&entry_string),
                ));
                log::trace!("ordering: {:?}, value: {value:?}", self.ordering);
                value
            })
    }

    fn index_of(&self, entry: &str) -> Option<usize> {
        let prefixes: Vec<&str> = Prefixer::new(entry).collect();
        let (real_prefixes, real_entry) = prefixes.split_at(prefixes.len() - 1);
        let (node, remainder) = TrieNode::find_last_match(self.store.to_owned(), real_prefixes);
        if !remainder.is_empty() {
            return None;
        }
        self.mapping
            .get(&TrieNodeStringPair(
                Rc::clone(&node),
                Rc::clone(&Rc::new(real_entry[0].to_string())),
            ))
            .cloned()
    }

    fn entry(&self, index: usize) -> Option<String> {
        if index < self.ordering.len() {
            Some(format!(
                "{}{}",
                self.ordering[index].0.as_ref().borrow(),
                self.ordering[index].1.as_ref(),
            ))
        } else {
            None
        }
    }
}

/// The [Prefixer] allows to split a given [&str] into its prefixes.
/// It is an [Iterator] an will iterate over the prefixes in the order they occur in the stringslice.
struct Prefixer<'a> {
    string: &'a str,
}

impl<'a> Prefixer<'a> {
    fn new(string: &'a str) -> Self {
        Self { string }
    }
}

impl<'a> Iterator for Prefixer<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.string.is_empty() {
            return None;
        }
        let mut splitpos: usize = self.string.len();
        if let Some(pos) = self.string.find("//") {
            splitpos = pos + 2;
        }
        if let Some(pos) = self.string.find(&['/', '#'][..]) {
            splitpos = pos + 1;
        }

        let (head, tail) = self.string.split_at(splitpos);
        self.string = tail;
        Some(head)
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use crate::physical::dictionary::Dictionary;

    use super::PrefixedStringDictionary;

    use test_log::test;

    fn create_dict() -> PrefixedStringDictionary {
        let mut dict = PrefixedStringDictionary::default();
        let vec: Vec<&str> = vec![
            "a",
            "b",
            "c",
            "a",
            "b",
            "c",
            "Position 3",
            "Position 4",
            "Position 3",
            "Position 5",
        ];

        for i in vec {
            dict.add(i.to_string());
        }
        dict
    }

    #[test]
    fn entry() {
        let dict = create_dict();
        assert_eq!(dict.entry(0), Some("a".to_string()));
        assert_eq!(dict.entry(1), Some("b".to_string()));
        assert_eq!(dict.entry(2), Some("c".to_string()));
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));
        assert_eq!(dict.entry(4), Some("Position 4".to_string()));
        assert_eq!(dict.entry(5), Some("Position 5".to_string()));
        assert_eq!(dict.entry(6), None);
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));
    }

    #[test]
    fn index_of() {
        let dict = create_dict();
        assert_eq!(dict.index_of("a".to_string().borrow()), Some(0));
        assert_eq!(dict.index_of("b".to_string().borrow()), Some(1));
        assert_eq!(dict.index_of("c".to_string().borrow()), Some(2));
        assert_eq!(dict.index_of("Position 3".to_string().borrow()), Some(3));
        assert_eq!(dict.index_of("Position 4".to_string().borrow()), Some(4));
        assert_eq!(dict.index_of("Position 5".to_string().borrow()), Some(5));
        assert_eq!(dict.index_of("d".to_string().borrow()), None);
        assert_eq!(dict.index_of("Pos".to_string().borrow()), None);
        assert_eq!(dict.index_of("Pos"), None);
        assert_eq!(dict.index_of("b"), Some(1));
    }

    #[test]
    fn properties() {
        let mut dict = create_dict();
        // no prefixes, so no children
        assert!(dict.store.as_ref().borrow().children().unwrap().is_empty());
        dict.add("https://wikidata.org/entity/Q42".to_string());
        // now we need some children
        assert!(!dict.store.as_ref().borrow().children().unwrap().is_empty());
        assert_eq!(
            dict.entry(6),
            Some("https://wikidata.org/entity/Q42".to_string())
        );
    }

    #[test]
    fn iri() {
        let mut dict = create_dict();
        // no prefixes, so no children
        assert!(dict.store.as_ref().borrow().children().unwrap().is_empty());
        dict.add("https://wikidata.org/entity/Q42".to_string());
        // now we need some children
        assert!(!dict.store.as_ref().borrow().children().unwrap().is_empty());
        assert_eq!(
            dict.entry(6),
            Some("https://wikidata.org/entity/Q42".to_string())
        );
        drop(dict);
        dict = PrefixedStringDictionary::default();
        let vec: Vec<&str> = vec![
            "https://wikidata.org/entity/Q42",
            "https://wikidata.org/entity/Q43",
            "https://wikidata.org/prop/direct/P31",
            "https://wikidata.org/prop/indirect/P31",
            "https://example.org/entity/Q42",
            "https://wikidata.org/prop/direct/P8810",
        ];

        for (i, str) in vec.iter().enumerate() {
            assert_eq!(dict.add(str.to_string()), i);
        }
        // duplicates
        for (i, str) in vec.iter().enumerate() {
            assert_eq!(dict.add(str.to_string()), i);
        }

        for (id, result) in vec.iter().enumerate() {
            assert_eq!(dict.entry(id).unwrap(), result.to_string());
            assert_eq!(dict.index_of(result), Some(id));
        }
    }
}
