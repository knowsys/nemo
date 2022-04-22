pub trait FloorToUsize {
    fn floor_to_usize(self) -> Option<usize>;
}

impl FloorToUsize for usize {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self)
    }
}

impl FloorToUsize for u64 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for u32 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for u16 {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self.into())
    }
}

impl FloorToUsize for u8 {
    fn floor_to_usize(self) -> Option<usize> {
        Some(self.into())
    }
}

impl FloorToUsize for i64 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i32 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i16 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}

impl FloorToUsize for i8 {
    fn floor_to_usize(self) -> Option<usize> {
        self.try_into().ok()
    }
}
