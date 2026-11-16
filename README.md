# CompactIdAllocator

`CompactIdAllocator` is a thread-safe allocator of compact, reusable integer IDs, implemented as a single self-contained C# class (`CompactIdAllocator.cs`).

It hands out IDs densely starting from 0. IDs that are no longer needed can be returned and are then reused, so the set of IDs in use stays compact — small enough to be used directly as indices into arrays or lists. Internally, allocation state is kept in bitmaps of 32 IDs per chunk. The storage grows automatically when more IDs are needed, and when the highest chunks become completely free, a shrink process trims the recorded maximum and raises the `OnShrink` event.

## Usage

```csharp
var allocator = new CompactIdAllocator(initialSize: 1024);

int id = allocator.Rent();   // rent an ID
// ... use id (e.g. as an index) ...
allocator.Return(id);        // return it for reuse

allocator.OnShrink += newMax => { /* the recorded maximum shrank */ };
```

- The constructor takes an initial size hint; the allocator grows beyond it on demand.
- `Rent()` returns the smallest available ID it can reach and is safe to call concurrently from multiple threads.
- `Return(id)` makes an ID available again. Returning an ID that was not rented out, or returning the same ID twice, is not validated — callers must return each rented ID exactly once.
- The class is `internal` by design: copy the source file into your own project and adjust the namespace as needed.
- Target framework of this project: .NET Standard 2.1, C# 12, nullable reference types enabled.

## Dependencies

`CompactIdAllocator` depends on `ConditionChangeNotifier` (namespace `Gownri.Threading`), which is currently included in this repository as `ConditionChangeNotifier.cs`. It may be split out into a separate project or repository in the future.

## Note

The description in this README was summarized by Mint, an AI assistant, based on the source code. Edits made by Mint are committed through the `mint-sharp` machine account.
