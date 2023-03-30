macro_rules! generic_trait_impl {
    ($storage:expr) => {
        generic_commit_impl!();
        generic_forget_impl!();
        generic_finalize_impl!($storage);
    };
}

macro_rules! generic_commit_impl {
    () => {
        fn commit(&mut self) {
            if let Some(value) = self.value.take() {
                self.vec.push(value)
            }
        }
    };
}
macro_rules! generic_forget_impl {
    () => {
        fn forget(&mut self) {
            self.value = None;
        }
    };
}
macro_rules! generic_finalize_impl {
    ($storage:expr) => {
        fn finalize(mut self: Box<Self>) -> VecT {
            self.as_mut().commit();
            $storage(self.vec)
        }
    };
}
