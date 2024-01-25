# TODO

- Also, Arc<TransactionManager> shouldn't have to be passed around like that... Maybe there should just be
  a separate mechanism just for reading stuff, and for writing the managers should take in a &mut Transaction.
- Rework error structure once the public api is more established
