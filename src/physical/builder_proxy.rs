//! The physical builder proxy takes values of some input type `T` and provides functionality to store them in a ['VecT']
use std::cell::RefCell;

use crate::error::Error;

use crate::physical::{
    datatypes::{storage_value::VecT, Double, Float},
    dictionary::Dictionary,
    management::database::Dict,
};

/// Trait for a Builder Proxy, which translates a value for a particular `DataValue` to the corresponding [`StorageType`][crate::physical::datatypes::StorageTypeName] eolumn elements
///
/// The intended logic for this trait is as follows:
/// - [`add`][Self::add]: caches a value to be added to the `StorageType`. If a value is already in the cache, the previously cached value will be [`commited`][Self::commit].
/// - [`forget`][Self::forget]: forgets the cached value if one exists.
/// - [`commit`][Self::commit]: takes the value which is cached and stores it in the column.
pub trait ColumnBuilderProxy<T>: std::fmt::Debug {
    /// Cache a value to be added to the ColumnBuilder. If a value is already cached, the cached value will be added to the ColumnBuilder before the new value is checked.
    ///
    /// The add function does check if the given input can be cast or parsed into the ColumnBuilder type.
    /// If this is not possible a corresponding [`Error`] is returned.
    fn add(&mut self, input: T) -> Result<(), Error>;
    /// Forgets a cached value.
    fn forget(&mut self);
    /// Commits the data, cleaning the cached value while adding it to the respective ColumnBuilder.
    fn commit(&mut self);
}

/// Generic trait implementation of the methods [`commit`][ColumnBuilderProxy::commit] and [`forget`][ColumnBuilderProxy::forget]
macro_rules! generic_trait_impl_without_add {
    ($storage:expr) => {
        fn commit(&mut self) {
            if let Some(value) = self.value.take() {
                self.vec.push(value)
            }
        }

        fn forget(&mut self) {
            self.value = None;
        }
    };
}

/// Generic trait implementation, if the value type can be cast implicitly into the physical builder value type
macro_rules! physical_generic_trait_impl {
    ($type:ty, $storage:expr) => {
        impl ColumnBuilderProxy<$type> for PhysicalGenericColumnBuilderProxy<$type> {
            generic_trait_impl_without_add!($storage);

            fn add(&mut self, input: $type) -> Result<(), Error> {
                self.commit();
                self.value = Some(input);
                Ok(())
            }
        }

        impl PhysicalColumnBuilderProxy<$type> for PhysicalGenericColumnBuilderProxy<$type> {
            fn finalize(mut self) -> VecT {
                self.commit();
                $storage(self.vec)
            }
        }
    };
}

/// Trait for Builder Proxy for the physical layer
///
/// This trait allows to get a [`VecT`] representation of a [`PhysicalColumnBuilderProxy`].
pub trait PhysicalColumnBuilderProxy<T>: ColumnBuilderProxy<T> {
    /// Writes the remaining prepared value and returns a VecT
    ///
    /// This will call [`commit`][ColumnBuilderProxy::commit] of the [`ColumnBuilderProxy`].
    /// # Note
    /// Rollbacks, if necessary, need to be done before this method is called.
    fn finalize(self) -> VecT;
}

/// [`PhysicalColumnBuilderProxy`] to add Strings
#[derive(Debug)]
pub struct PhysicalStringColumnBuilderProxy<'a> {
    dict: &'a RefCell<Dict>,
    value: Option<u64>,
    vec: Vec<u64>,
}

impl<'a> PhysicalStringColumnBuilderProxy<'a> {
    /// Create a new [`PhysicalStringColumnBuilderProxy`] with the given [`dictionary`][Dict]
    pub fn new(dict: &'a RefCell<Dict>) -> Self {
        Self {
            dict,
            value: Default::default(),
            vec: Default::default(),
        }
    }
}

impl ColumnBuilderProxy<String> for PhysicalStringColumnBuilderProxy<'_> {
    generic_trait_impl_without_add!(VecT::U64);
    fn add(&mut self, input: String) -> Result<(), Error> {
        self.commit();
        self.value = Some(self.dict.borrow_mut().add(input).try_into()?);
        Ok(())
    }
}

impl PhysicalColumnBuilderProxy<String> for PhysicalStringColumnBuilderProxy<'_> {
    fn finalize(mut self) -> VecT {
        self.commit();
        VecT::U64(self.vec)
    }
}

/// [`PhysicalColumnBuilderProxy`] to add types without special requirements (e.g. dictionary)
#[derive(Default, Debug)]
pub struct PhysicalGenericColumnBuilderProxy<T> {
    value: Option<T>,
    vec: Vec<T>,
}

physical_generic_trait_impl!(u64, VecT::U64);
physical_generic_trait_impl!(i64, VecT::I64);
physical_generic_trait_impl!(u32, VecT::U32);
physical_generic_trait_impl!(Float, VecT::Float);
physical_generic_trait_impl!(Double, VecT::Double);

/// Enum Collection of all physical builder proxies
#[derive(Debug)]
pub enum PhysicalBuilderProxyEnum<'a> {
    /// Proxy for String Type
    String(PhysicalStringColumnBuilderProxy<'a>),
    /// Proxy for I64 Type
    I64(PhysicalGenericColumnBuilderProxy<i64>),
    /// Proxy for U64 Type
    U64(PhysicalGenericColumnBuilderProxy<u64>),
    /// Proxy for U32 Type
    U32(PhysicalGenericColumnBuilderProxy<u32>),
    /// Proxy for Float Type
    Float(PhysicalGenericColumnBuilderProxy<Float>),
    /// Proxy for Double Type
    Double(PhysicalGenericColumnBuilderProxy<Double>),
}

impl PhysicalBuilderProxyEnum<'_> {
    /// Finalize the wrapped member and returns its respective VecT
    pub fn finalize(self) -> VecT {
        match self {
            PhysicalBuilderProxyEnum::String(bp) => bp.finalize(),
            PhysicalBuilderProxyEnum::I64(bp) => bp.finalize(),
            PhysicalBuilderProxyEnum::U64(bp) => bp.finalize(),
            PhysicalBuilderProxyEnum::U32(bp) => bp.finalize(),
            PhysicalBuilderProxyEnum::Float(bp) => bp.finalize(),
            PhysicalBuilderProxyEnum::Double(bp) => bp.finalize(),
        }
    }
}
