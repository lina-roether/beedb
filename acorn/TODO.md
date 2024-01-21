# TODO

- I'm pretty sure SegmentAllocManager is broken as it stands...
  It stores its state inside the segment's header page, while it should really keep track
  of allocation structures in memory, because otherwise the state won't be synchronized until the allocation
  finishes
- Rework error structure once the public api is more established
