// todo: split project

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Gownri.Threading;

public class ConditionChangeNotifier()
{
    private class VersionSource
    {
        private const nint sentinelVersion = ~0;
        private const nint maxCount = sentinelVersion - 1;
        internal const nint initialVersion = 1;
        internal static VersionSource Sentinel { get; } = new() { version = sentinelVersion, };

        private nint version = initialVersion;
        internal nint Version
            => Volatile.Read(ref this.version);

        internal (VersionSource source, long value) Increment()
        {

            var v = this.version;
            do
            {
                if ((ulong)v >= unchecked((ulong)maxCount))
                {
                    Volatile.Write(ref this.version, sentinelVersion);
                    return (new(), initialVersion);
                }
                if (v == (v = Interlocked.CompareExchange(ref this.version, v + 1, v)))
                    return (this, v + 1);
            } while (true);
        }

    }
    private readonly struct VersionLocal : IEquatable<VersionLocal>, IEquatable<VersionSource>
    {
        private readonly VersionSource versionSource;
        private readonly nint versionValue;

        public VersionLocal(VersionSource src)
        {
            this.versionSource = src;
            this.versionValue = src.Version;
        }

        public readonly bool Equals(VersionLocal other)
            => this.versionValue == other.versionValue
            && object.ReferenceEquals(this.versionSource, other.versionSource);
        public readonly bool Equals(VersionSource source)
        {
            var srcValue = source.Version;
            return this.versionValue == srcValue
            && object.ReferenceEquals(this.versionSource, source);
        }

        public readonly override bool Equals(object? obj)
            => obj is VersionLocal self && this.Equals(self);
        public readonly override int GetHashCode()
            => HashCode.Combine(this.versionValue, this.versionSource);

    }

    public ref struct Scope
    {
        private static Action<object> CancellationCallback { get; } = static obj =>
        {
            if (obj is not ManualResetEventSlimNode myNode)
                return;
            Cancel(myNode);
        };
        private ManualResetEventSlimNode? myNode;

        public readonly bool IsClosed
            => this.myNode is null;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Scope(object? myNode)// Accessibility Bypass
        {
            this.myNode = (ManualResetEventSlimNode?)myNode;
        }

        public bool WaitPulse(CancellationToken token = default)
        {
            var myNode = this.myNode;
            if (myNode is null)
#if DEBUG
                throw new ObjectDisposedException(typeof(Scope).FullName);
#else
                return false;
#endif
            var pub = myNode.publisher;
            if (pub.IsSet)
                return false;
            using var cc =
#if NETCOREAPP3_0_OR_GREATER
            token.UnsafeRegister(CancellationCallback, myNode);
#else
            token.Register(CancellationCallback, myNode);
#endif
            if (pub.CompareJoin(myNode, myNode.version))
                myNode.Wait();
            myNode.version = new(Volatile.Read(ref pub.versionSource));
            token.ThrowIfCancellationRequested();
            return true;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            var node = this.myNode;
            if (node is null)
                return;
            //Cancel(node);
            this.myNode = null;
            TLSNode = node;
            return;
        }

        private static void Cancel(ManualResetEventSlimNode myNode)
        {
            if (myNode.IsSet is false)
            {
                myNode.version = default;
                myNode.publisher.Flash();
            }
        }
    }
    
    private class ManualResetEventSlimNode : ManualResetEventSlim
    {
#if DEBUG
        private int threadID = Environment.CurrentManagedThreadId;
        public new void Wait()
        {
            System.Diagnostics.Debug.Assert(this.threadID == Environment.CurrentManagedThreadId);
            base.Wait();
        }
#endif
        internal ManualResetEventSlimNode? next;
        internal ConditionChangeNotifier publisher = null!;
        internal VersionLocal version;
        public ManualResetEventSlimNode(bool initialState)
            : base(initialState)
        { }
    }

    [ThreadStatic]
    private static ManualResetEventSlimNode? TLSNode;

    private const long leaderFlag = long.MinValue;
    private const long pulseFlag = long.MinValue >>> 1;
    private const long flashFlag = long.MinValue >>> 2;
    private const long countMask = ~(leaderFlag | pulseFlag | flashFlag);
    private long count;
    private bool IsSet
        => (Volatile.Read(ref this.count) & countMask) == 0L;
    private ManualResetEventSlimNode? header;
    private  VersionSource versionSource = new();
    public Scope EnterScope()
    {
        var vSrc = this.versionSource;
        var myWaiter = TLSNode ??= new(true);
        TLSNode = null;
        myWaiter.version = new(vSrc);
        myWaiter.publisher = this;
        return new(myWaiter);
    }

    public void Open()
    {
        var comparand = this.count;
        while (true)
        {
            if ((ulong)(uint)(comparand & countMask) >= (ulong)(uint)int.MaxValue)
                Throw();
            static void Throw()
                => throw new InvalidOperationException();
            if (comparand == (comparand = Interlocked.CompareExchange(ref this.count, comparand + 1L, comparand)))
                return;
        }
    }
    
    public void Close(bool forcePulse = true)
    {
        var comparand = this.count;
        while (true)
        {
            var count = comparand & countMask;
            if (!forcePulse && count > 1L)
            {
                if (comparand == (comparand = Interlocked.CompareExchange(ref this.count, comparand - 1L, comparand)))
                    return;
            }
            else
            {
                if ((comparand & countMask) == 0L)
                    Throw();
                static void Throw()
                    => throw new InvalidOperationException();
                if (comparand != (comparand = Interlocked.CompareExchange(ref this.count, (comparand - 1L) | pulseFlag | leaderFlag, comparand)))
                    continue;

                if ((comparand & leaderFlag) == default)
                    this.LeaderProcess();
                return;
            }
        }
    }
    public void PulseAll()
    {
        //var versionSrc = versionSource;
        //var (after, _) = versionSrc.Increment();
        //Interlocked.CompareExchange(ref this.versionSource, after, versionSrc);
        var comparand = this.count;
        while (true)
        {
            if (comparand == (comparand = Interlocked.CompareExchange(ref this.count, comparand | pulseFlag | leaderFlag, comparand)))
            {
                if ((comparand & leaderFlag) == default)
                    this.LeaderProcess();
                return;
            }
        }
    }
    private bool Flash()
    {
        var comparand = this.count;
        while (comparand != (comparand = Interlocked.CompareExchange(ref this.count, comparand | flashFlag | leaderFlag, comparand)))
            ;
        if ((comparand & leaderFlag) != default)
            return false;

        this.LeaderProcess();
        return true;
    }
    private void LeaderProcess()
    {
        var comparand = Volatile.Read(ref this.count);
        while (true)
        {
            var version = this.versionSource;
            var versionLocal = new VersionLocal(version);

            var requests = comparand & (pulseFlag | flashFlag);
            comparand = Interlocked.Add(ref this.count, -requests);
            if ((requests & pulseFlag) != default)
            {
                var (versionAfter, _) = version.Increment();
                if (object.ReferenceEquals(version, version = Interlocked.CompareExchange(ref this.versionSource, versionAfter, version)))
                {
                    version = versionAfter;
                    versionLocal = new(version);
                }
            }
#if DEBUG
            System.Diagnostics.Debug.Assert((comparand & leaderFlag) != default);
#endif
            ManualResetEventSlimNode? head;
            while (true)
            {
                head = Volatile.Read(ref this.header);
                if (head is null)
                    break;
                if (versionLocal.Equals(head.version) is false)
                {
                    if (object.ReferenceEquals(head, Interlocked.CompareExchange(ref this.header, head.next, head)) is false)
                        continue;

                    head.Set();
                    continue;
                }
                
                var prev = head;
                var curr = prev.next;
                while (true)
                {
                    if (curr is null)
                        break;
                    if (versionLocal.Equals(curr.version))
                    {
                        prev = curr;
                        curr = curr.next;
                    }
                    else
                    {
                        var dispose = curr;
                        curr = (prev.next = curr.next);
                        dispose.Set();
                    }
                }
                break;
            }

            do if (comparand == (comparand = Interlocked.CompareExchange(ref this.count, comparand & ~leaderFlag, comparand)))
            {
                if (object.ReferenceEquals(head, Volatile.Read(ref this.header)))
                    return;
                comparand &= ~leaderFlag;
                while (comparand != (comparand = Interlocked.CompareExchange(ref this.count, comparand | leaderFlag, comparand)))
                    if ((comparand & leaderFlag) != default)
                        return;
                break;
            } while ((comparand & (pulseFlag | flashFlag)) != default);
            comparand |= leaderFlag;
            continue;
        }
    }

    private bool CompareJoin(ManualResetEventSlimNode nodeValue, VersionLocal comparandVersion)
    {
        var vSrc = this.versionSource;
        var head = this.header;
        nodeValue.Reset();
        while (true)
        {
            var vLocal = new VersionLocal(vSrc);
            if (this.IsSet || vLocal.Equals(comparandVersion) is false)
                return false;
            nodeValue.next = head;
            if (object.ReferenceEquals(head, head = Interlocked.CompareExchange(ref this.header, nodeValue, head)) is false)
                continue;

            vLocal = new(Volatile.Read(ref this.versionSource));
            if (this.IsSet is false && vLocal.Equals(comparandVersion))
                return true;
            
            if (this.Flash() is false)
                nodeValue.Wait();
            return false;
        }

    }
}




//internal class IDList
//{
//    private readonly Lock lockObj = new();
//    internal event Action<int>? OnShrink;
//    private int[] freeIDs = [];
//#if DEBUG
//    internal int[] Debug_FreeIDs
//        => this.freeIDs;
//#endif
//    private int freeCount = 0;
//    private int length = 0;
//    internal int FreeCount
//        => this.freeCount;
//    internal int MaxRentedID
//    {
//        get
//        {
//            var arr = this.freeIDs;
//            var max = this.length;
//            var count = this.freeCount;
//            if ((uint)count > (uint)arr.Length)
//                return max;
//            var span = arr.AsSpan(0, count);
//            foreach (var v in span)
//                if (v == max)
//                    max = v - 1;
//                else
//                    break;
//            return max;
//        }
//    }

//    private void InvokeShrinkEvent()
//    {
//        using var lockScope = this.lockObj.EnterScope();
//        var max = this.MaxRentedID;
//        this.OnShrink?.Invoke(max);
//    }
//    internal int Rent()
//    {
//        using var lockScope = this.lockObj.EnterScope();
//        var freeCount = this.freeCount;
//        if (freeCount == 0)
//            return Interlocked.Increment(ref this.length);
//        var fArr = this.freeIDs;
//        var fIdx = this.freeCount - 1;
//        if ((uint)fIdx < (uint)fArr.Length)
//        {
//            var ret = fArr[fIdx];
//            Volatile.Write(ref this.freeCount, fIdx);
//            return ret;
//        }
//#if DEBUG
//        System.Diagnostics.Debug.Fail(nameof(Rent));
//#endif
//        return Interlocked.Increment(ref this.length);
//    }

//    private readonly struct DESC : IComparer<int>
//    {
//        public int Compare(int x, int y)
//            => y.CompareTo(x);
//    }
//    internal int Return(int id)
//    {
//#if DEBUG
//        System.Diagnostics.Debug.Assert(id != 0);
//#endif
//        using var lockScope = this.lockObj.EnterScope();
//        var fIDs = this.freeIDs;
//        var count = this.freeCount;
//        do
//        {
//            if ((uint)count < (uint)fIDs.Length)// count + 1
//                break;
//            const int freeIDsInitialSize = 4;
//            var newFreeIDs = new int[Math.Max(fIDs.Length * 2, freeIDsInitialSize)];
//            var __debug = fIDs.AsSpan().TryCopyTo(newFreeIDs);
//#if DEBUG
//            System.Diagnostics.Debug.Assert(__debug);
//#endif
//            fIDs = this.freeIDs = newFreeIDs;
//        } while (true);
//        var span = fIDs.AsSpan(0, count);
//        var insert = span.BinarySearch(id, default(DESC));
//#if DEBUG
//        System.Diagnostics.Debug.Assert(insert < 0);
//#endif
//        insert = ~insert;
//        if ((uint)insert >= (uint)span.Length)
//        {
//#if DEBUG
//            System.Diagnostics.Debug.Assert(insert == count);
//#endif
//            fIDs[count] = id;
//        }
//        else
//        {
//            var dstStart = insert + 1;
//            var dst = (uint)dstStart <= (uint)fIDs.Length
//                ? fIDs.AsSpan(dstStart)
//                : default;

//            var ___debug = span.Slice(insert).TryCopyTo(dst);
//#if DEBUG
//            System.Diagnostics.Debug.Assert(___debug);
//#endif
//            span[insert] = id;
//        }
//        ++count;
//        Volatile.Write(ref this.freeCount, count);
//        return count;
//    }


//    //internal Lock.Scope BlockRentEnterScope()
//    //    => this.lockObj.EnterScope();
//}
