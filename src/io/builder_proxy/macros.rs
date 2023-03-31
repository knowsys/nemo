macro_rules! generic_trait_impl {
    ($storage:expr) => {
        fn commit(&mut self) {
            if let Some(value) = self.value.take() {
                self.vec.push(value)
            }
        }

        fn forget(&mut self) {
            self.value = None;
        }

        fn finalize(mut self: Box<Self>) -> VecT {
            self.as_mut().commit();
            $storage(self.vec)
        }
    };
}
