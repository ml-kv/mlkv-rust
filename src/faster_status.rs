pub enum FasterStatus {
  OK = 0,
  Pending = 1,
  NotFound = 2,
  OutOfMemory = 3,
  IOError = 4,
  Corruption = 5,
  Aborted = 6,
}
