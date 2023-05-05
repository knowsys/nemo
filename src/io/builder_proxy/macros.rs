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

macro_rules! logical_generic_trait_impl {
    () => {
        fn commit(&mut self) {
            self.physical.commit()
        }

        fn forget(&mut self) {
            self.physical.forget()
        }
    };
}
