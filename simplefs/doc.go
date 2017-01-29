package simplefs

//a neat fuse reference: https://www.cs.hmc.edu/~geoff/classes/hmc.cs135.201109/homework/fuse/fuse_doc.html#function-purposes

//the why of simplefs
// - be portable! why cant we share data by sharing our filesystems
// - be extendable! why cant developers hook into the filesystem for awesome
// - be collaborative! (usable) locking as a first class citizen
// - be usable! provide a FUSE interface on OSX, Windows and Linux
