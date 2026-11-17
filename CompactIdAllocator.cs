//#define LASTLOCKER
#define EVENT_GROWING
using Gownri.Threading;
using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Gownri.IndexFriendly;

public class CompactIdAllocator
{
#if DEBUG
    internal ulong RentCursor
        => (ulong)this.rentCursor;
    internal int[] Bitmaps
        => this.bitmaps;
    internal int Max
        => SyncProcCtxToMax(this.shrinkProcessContext);

#endif
#if DEBUG || LASTLOCKER
    private int lastLockerThreadID = 0;
    internal int LastLockerThreadID
        => this.lastLockerThreadID;

    private void Enter()
    {
        var prev = Interlocked.Exchange(ref this.lastLockerThreadID, Environment.CurrentManagedThreadId);
        if (prev != 0)
            throw new InvalidOperationException($"{Environment.CurrentManagedThreadId}: {prev}");
    }

    private void Exit()
    {
        var exitThread = Interlocked.Exchange(ref this.lastLockerThreadID, 0);
        if (exitThread != Environment.CurrentManagedThreadId)
            throw new InvalidOperationException($"{Environment.CurrentManagedThreadId}: {exitThread}");
    }
#endif
    private static int overArrayLength => Math.Clamp(System.Numerics.Vector<int>.Count, 1, int.MaxValue);
    //private static int initialArraySize
    //    => Math.Clamp(Vector<int>.Count, 1, int.MaxValue);
    private const int IndexPerBitmapOffset = 5;
    private const int bitmapSize = sizeof(int) * 8;
    private const int full = unchecked(~0);

    //private long returnerCountAndMax;

    //private const long arrayLockFlag = long.MinValue >>> 5;
    //private const long cursorLockFlag = long.MinValue >>> 4;
    private const long lockFlag = long.MinValue;
    private const long modifierIncrement = long.MinValue >>> IndexPerBitmapOffset;
    private const int modifierCountOffset = bitmapSize + (sizeof(int) * 8) - IndexPerBitmapOffset - 1;
    private const long modifierCountMask = unchecked(lockFlag - modifierIncrement);
    private const int modifierCountMaxValue = (int)(modifierCountMask >>> modifierCountOffset);
    //private const long thinReturnerCountIncrement = long.MinValue >>> 3;
    private const long indexMask = ~(lockFlag | modifierCountMask | (long)uint.MaxValue);
    private const long cursorContextMask = lockFlag | modifierCountMask;
    //private const long thinReturnerCountMask = -thinReturnerCountIncrement;


    private long rentCursor;
    private static WaitCallback WorkerItem { get; } = static self => (self as CompactIdAllocator)?.ShrinkProcess(true);

    private const int interceptedIncrement = 1;
    private int interceptedCount;

    private const int processing = int.MinValue;
    //private const int fetchRequest = int.MinValue >>> 1;
    private const int shrinkRequest = int.MinValue >>> 2;
    private const int suspendShrinkRequest = int.MinValue >>> 3;
    private const int shrinking = int.MinValue >>> 4;
    private const int maxMask = int.MaxValue / (sizeof(int) * 8);
    private int shrinkProcessContext;


    private int[] bitmaps;
    private int[][] overField = [];

    private readonly ConditionChangeNotifier conditionEvent = new();
    public event Action<int>? OnShrink;

    private AggregateException? ex;
    public CompactIdAllocator(int initialSize)
    {
        var (q, r) = DivRem(initialSize, bitmapSize);
        if (r != 0)
            q++;
        var arr = this.bitmaps = new int[q];
        if (arr.Length == 0)
            this.rentCursor = default;
        else
        {
            arr[0] = full;
            this.rentCursor = unchecked((long)(uint)full);
        }
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int q, int r) DivRem(int left, int right)
#if NET6_OR_GREATER
        => Math.DivRem(left, right);
#else
    {
        var quot = Math.DivRem(left, right, out var rem);
        return (quot, rem);
    }
#endif
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RentCursorToIndex(long rentCursor)
    => (int)((rentCursor & indexMask) >>> bitmapSize);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RentCursorToModifierCount(long rentCursor)
        => (int)((rentCursor & modifierCountMask) >>> (bitmapSize + (sizeof(int) * 8) - IndexPerBitmapOffset - 1));
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SyncProcCtxToMax(int ctx)
        => ctx & maxMask;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int AcceptIntercepted()
    {
        return Interlocked.Exchange(ref this.interceptedCount, 0);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void NotifyIntercepted()
    {
        Interlocked.Add(ref this.interceptedCount, interceptedIncrement);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static long AcceptedComparand(long comparand, int acceptances)
           =>
#if DEBUG
           acceptances > modifierCountMaxValue
               ? throw new OverflowException()
               :
#endif
           (comparand & ~modifierCountMask) | ((long)(uint)acceptances << modifierCountOffset);

    private long AcceptAndUnlock(long comparand, int acceptances)
    {
        while (true)
        {
            comparand = AcceptedComparand(comparand, acceptances);
#if DEBUG || LASTLOCKER
            Exit();
#endif
            if (comparand == (comparand = Interlocked.CompareExchange(ref this.rentCursor, comparand & ~(lockFlag | modifierCountMask), comparand)))
                return comparand;
#if DEBUG || LASTLOCKER
            Enter();
#endif
            Thread.Yield();
            acceptances += this.AcceptIntercepted();
        }

    }
    //[MethodImpl(MethodImplOptions.NoInlining)]
    private (int[] arr, int localIdx, int overIdx) ComputeGetArray(int idx)
    {
        var primaryArray = Volatile.Read(ref this.bitmaps);
        if ((uint)idx < (uint)primaryArray.Length)
            return (primaryArray, idx, -1);
        var (q, r) = DivRem(idx - primaryArray.Length, overArrayLength);
        var over = Volatile.Read(ref this.overField);
        return (uint)q < (uint)over.Length
            ? (over[q], r, q)
            : ([], 0, -1);
    }
    public int Rent()
    {
        var comparand = Volatile.Read(ref this.rentCursor);
        while (true)
        {
            
            var bitmap = (int)comparand;
            if (bitmap == default)
            {
                Fetch(comparand);
                comparand = Volatile.Read(ref this.rentCursor);
                continue;
            }
#if NETCOREAPP3_0_OR_GREATER || NET5_OR_GREATER
            var ret = System.Numerics.BitOperations.TrailingZeroCount((uint)bitmap);
#else
            var ret = 0;
            for (; ((uint)bitmap & (1u << ret)) == 0u; ++ret)
                ;
#endif
            if (comparand == (comparand = Interlocked.CompareExchange(ref this.rentCursor, comparand & ~(1L << ret), comparand)))
                return (RentCursorToIndex(comparand) * bitmapSize) + ret;
        }
    }

    private void Fetch(long comparand)
    {
        var (_, hint) = this.MoveCursor(comparand);
        if (hint)
            this.ShrinkHint();
    }

    public void Return(int id)
    {
        var (returnIdx, bit) = DivRem(id, bitmapSize);
        bit = 1 << bit;
        var cursorComparand = this.rentCursor;
        while (true)
        {
            var cursorIdx = RentCursorToIndex(cursorComparand);
            if (cursorIdx == returnIdx)
            {
                if (cursorComparand
                    == (cursorComparand = Interlocked.CompareExchange(ref this.rentCursor, cursorComparand | (long)(uint)bit, cursorComparand)))
                    return;
                else
                    continue;
            }


            if ((uint)cursorComparand == default && cursorIdx > returnIdx)
                if (cursorComparand
                    == (cursorComparand = Interlocked.CompareExchange(
                        ref this.rentCursor,
                        (cursorComparand & cursorContextMask) | ((long)(uint)returnIdx << bitmapSize) | (long)(uint)bit,
                        cursorComparand)
                    ))
                    return;
                else
                    continue;


            var arr = this.bitmaps;
            var localIdx = returnIdx;
            if (cursorIdx < returnIdx)
            {
                while (true)
                    if ((uint)localIdx < (uint)arr.Length)
                        break;
                    else
                        (arr, localIdx, _) = ComputeGetArray(returnIdx);

                var inArrBits = arr[localIdx];
                while(inArrBits != default)
                    if (inArrBits == (inArrBits = Interlocked.CompareExchange(ref arr[localIdx], inArrBits & ~bit, inArrBits)))
                    {
                        if ((inArrBits & ~bit) == default)
                            this.ShrinkHint();
                        return; 
                    }
            }

            if ((cursorComparand & modifierCountMask) == modifierCountMask)
            {
                cursorComparand = WaitOverModifiers();
                long WaitOverModifiers()
                {
                    long ret;
                    using var scope = this.conditionEvent.EnterScope();
                    while (((ret = Volatile.Read(ref this.rentCursor)) & modifierCountMask) == modifierCountMask)
                        scope.WaitPulse();
                    return ret;
                }
                continue;
            }

#if DEBUG
            System.Diagnostics.Debug.Assert(cursorIdx != returnIdx);
#endif
            if (cursorIdx > returnIdx)
            {
                var newLessValue = ((cursorComparand + modifierIncrement) & cursorContextMask) | ((long)(uint)returnIdx << bitmapSize) | (long)(uint)bit;
                if (cursorComparand != (cursorComparand = Interlocked.CompareExchange(ref this.rentCursor, newLessValue, cursorComparand)))
                    continue;
                returnIdx = RentCursorToIndex(cursorComparand);
                localIdx = returnIdx;
                bit = (int)cursorComparand;
                cursorComparand = (newLessValue & ~lockFlag) | (cursorComparand & lockFlag);
            }
            else
            {
                if (cursorComparand != (cursorComparand = Interlocked.CompareExchange(ref this.rentCursor, cursorComparand + modifierIncrement, cursorComparand)))
                    continue;
                cursorComparand += modifierIncrement;
            }
            while (true)
                if ((uint)localIdx < (uint)arr.Length)
                    break;
                else
                    (arr, localIdx, _) = ComputeGetArray(returnIdx);
            var arrBits = arr[localIdx];
            int after;
            while (arrBits != (arrBits = Interlocked.CompareExchange(ref arr[localIdx], after = arrBits & ~bit, arrBits)))
                ;

            while (true)
            {
                if ((cursorComparand & lockFlag) == default)
                {
                    if (cursorComparand == (cursorComparand =
                        Interlocked.CompareExchange(
                            ref this.rentCursor,
                            cursorComparand - modifierIncrement,
                            cursorComparand
                        )
                    ))
                        break;
                }
                else
                {
                    this.NotifyIntercepted();
                    break;
                }
            }
            if (after == default)
                this.ShrinkHint();
            return;
        }
    }
    private void ShrinkHint()
    {
        var syncCtx = this.shrinkProcessContext;
        while (syncCtx != (syncCtx = Interlocked.CompareExchange(ref this.shrinkProcessContext, syncCtx | processing | shrinkRequest, syncCtx)))
            ;
        if ((syncCtx & processing) == default)
            this.ShrinkProcess(false);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool WaitIfLock(long comparand)
    {
        if ((comparand & lockFlag) == default)
            return false;
        Lock();
        return true;
        void Lock()
        {
            var ev = this.conditionEvent;
            if ((Volatile.Read(ref this.rentCursor) & lockFlag) != default) lock (ev)
            {
                if ((Volatile.Read(ref this.rentCursor) & lockFlag) == default)
                    return;
                using var scope = ev.EnterScope();
                while ((Volatile.Read(ref this.rentCursor) & lockFlag) != default)
                    if (scope.WaitPulse() is false)
                        Thread.Yield();
            }
        }
    }

    private (int index, bool shrinkHint) MoveCursor(long comparand)
    {
        int idx;
        var shrinkHint = false;
        while (true)
        {
            idx = RentCursorToIndex(comparand);
            if ((int)comparand != default)
                return (idx, shrinkHint);
            if (this.WaitIfLock(comparand))
            {
                comparand = Volatile.Read(ref this.rentCursor);
                continue;
            }
            if (comparand == (comparand = Interlocked.CompareExchange(ref this.rentCursor, comparand | lockFlag, comparand)))
                break;
        }
#if DEBUG || LASTLOCKER

        Enter();
#endif
        comparand |= lockFlag;
        var acceptances = 0;
        do
        {
            var (arr, localIdx, overIdx) = ComputeGetArray(idx);

            while (true)
            {
                if ((uint)localIdx >= (uint)arr.Length)
                {
                    overIdx++;
                    var over = this.overField;
                    if ((uint)overIdx < (uint)over.Length)
                    {
                        arr = over[overIdx];
                        localIdx = 0;
                        continue;
                    }

                    comparand = Grow(comparand, idx, acceptances);
                    return (RentCursorToIndex(comparand), shrinkHint);
                }

                if (arr[localIdx] == full)
                {
                    idx++;
                    localIdx++;
                    continue;
                }

                var bits = Interlocked.Exchange(ref arr[localIdx], full);
                if (bits == full)
                {
#if DEBUG
                    System.Diagnostics.Debug.Fail(nameof(full));
#endif
                    break;
                }
                bits = ~bits;

                var maxCmp = this.shrinkProcessContext;
                while (true)
                {
                    int afterMax;
                    if (idx < SyncProcCtxToMax(maxCmp))
                        afterMax = maxCmp | suspendShrinkRequest;
                    else
                    {
                        if ((maxCmp & shrinking) != default)
                        {
                            maxCmp = Wait();
                            int Wait()
                            {
                                int ret;
                                var waiter = this.conditionEvent;
                                using var scope = waiter.EnterScope();
                                while (((ret = Volatile.Read(ref this.shrinkProcessContext)) & shrinking) != default)
                                    scope.WaitPulse();
                                return ret;
                            }
                            continue;
                        }
                        afterMax = (maxCmp & ~maxMask) | idx | suspendShrinkRequest;
                    }
                    if (maxCmp == (maxCmp = Interlocked.CompareExchange(ref this.shrinkProcessContext, afterMax, maxCmp)))
                        break;
                }
                comparand = AcceptedComparand(comparand, acceptances);
#if DEBUG || LASTLOCKER
                Exit();
#endif
                if (comparand == (comparand =
                    Interlocked.CompareExchange(
                        ref this.rentCursor,
                        ((long)(uint)idx << bitmapSize) | (long)(uint)bits,
                        comparand
                    )
                ))
                    return (idx, shrinkHint);
#if DEBUG || LASTLOCKER

                Enter();
#endif
                var bitsCmp = full;
#if DEBUG
                do
                    System.Diagnostics.Debug.Assert((bitsCmp & bits) == bits);
#endif
                while (bitsCmp != (bitsCmp = Interlocked.CompareExchange(ref arr[localIdx], bitsCmp & ~bits, bitsCmp)))
                    ;
                shrinkHint |= (bitsCmp & ~bits) == default;
                break;
            }

            acceptances += this.AcceptIntercepted();
            comparand = Volatile.Read(ref this.rentCursor);
            if ((int)comparand != default)
            {
                comparand = this.AcceptAndUnlock(comparand, acceptances);
                return (RentCursorToIndex(comparand), shrinkHint);
            }
        } while (true);
        long Grow(long comparand, int idx, int acceptances)
        {
#if EVENT_GROWING
            var waiter = this.conditionEvent;
            waiter.Open();
#endif
            try
            {
                var newArr = new int[overArrayLength];
                newArr[0] = full;
                if (idx == 0)
                {
                    Volatile.Write(ref this.bitmaps, newArr);
                }
                else
                {
                    var oldJ = this.overField;
                    Volatile.Write(ref this.overField, [.. oldJ, newArr]);
                }

                while (true)
                {
#if DEBUG || LASTLOCKER
                    Exit();
#endif
                    var ret = ((long)(uint)idx << bitmapSize) | (long)unchecked((uint)full);
                    comparand = AcceptedComparand(comparand, acceptances);
                    if (comparand == (comparand = Interlocked
                        .CompareExchange(
                            ref this.rentCursor,
                            ret,
                            comparand
                        )
                    ))
                        return ret;
                    else
                    {
#if DEBUG || LASTLOCKER
                        Enter();
#endif
                        if ((int)comparand != default)
                        {
                            if ((uint)0 < (uint)newArr.Length)
                                newArr[0] = default;
#if DEBUG
                            else
                                System.Diagnostics.Debug.Fail(nameof(MoveCursor));
#endif
                            comparand = this.AcceptAndUnlock(comparand, acceptances);
                            return comparand;
                        }

                        acceptances += this.AcceptIntercepted();
                    }
                }
            }
            catch
            {
                this.AcceptAndUnlock(comparand, acceptances);
                throw;
            }
#if EVENT_GROWING
            finally
            {
                waiter.Close();
            }
#endif
        }
    }
    private void ShrinkProcess(bool onWorkerThread)
    {
        var waiter = this.conditionEvent;
        var comparand = this.shrinkProcessContext;
        System.Collections.Generic.List<Exception>? exs = null;
        while (true)
        {
            var shrinkingTaken = false;
            var nowMax = SyncProcCtxToMax(comparand);
            try
            {
#if DEBUG
                System.Diagnostics.Debug.Assert((comparand & processing) != default);
#endif
                var requests = comparand & (suspendShrinkRequest | shrinkRequest);
                comparand = Interlocked.Add(ref this.shrinkProcessContext, -requests);
                var prevMax = nowMax;
                var (arr, localIdx, overIdx) = ComputeGetArray(nowMax);
                var over = this.overField;
                while (true)
                {
                    if ((uint)localIdx >= (uint)arr.Length)
                    {
                        overIdx--;
                        if ((uint)overIdx >= (uint)over.Length)
                            if (nowMax <= 0)
                            {
                                nowMax = 0;
                                break;
                            }
                            else
                            {
                                arr = this.bitmaps;
                                localIdx = arr.Length - 1;
#if DEBUG
                                System.Diagnostics.Debug.Assert(nowMax == localIdx);
#endif
                                continue;
                            }
                        arr = over[overIdx];
                        localIdx = arr.Length - 1;
                        continue;
                    }
                    if (arr[localIdx] != default)
                        break;
                    localIdx--;
                    nowMax--;
                }
                if (nowMax < prevMax)
                {
#if DEBUG
                    System.Diagnostics.Debug.Assert((comparand & (suspendShrinkRequest | shrinkRequest)) == default);
#endif
                    if (onWorkerThread)
                    {
                        if (comparand != (comparand = Interlocked.CompareExchange(ref this.shrinkProcessContext, nowMax | shrinking | processing, comparand)))
                            continue;
                    }
                    else
                    {
                        if (comparand != (comparand = Interlocked.CompareExchange(ref this.shrinkProcessContext, (nowMax + 1) | processing, comparand)))
                            continue;
                        ThreadPool.UnsafeQueueUserWorkItem(WorkerItem, this);
                        return;
                    }
                    waiter.Open();
                    shrinkingTaken = true;
                    Volatile.Read(ref this.OnShrink)?.Invoke(nowMax);
                }
            }
            catch (Exception e)
            {
                exs ??= new();
                exs.Add(e);
                shrinkingTaken = (Volatile.Read(ref shrinkProcessContext) & shrinking) != default;
            }
            finally
            {
                if (shrinkingTaken)
                {
                    comparand = Interlocked.Add(ref this.shrinkProcessContext, -shrinking);
                    waiter.Close();
                }
            }

            if (comparand != (comparand = Interlocked.CompareExchange(ref this.shrinkProcessContext, comparand & ~processing, comparand)))
                continue;

            if (exs is not null)
            {
                var existingCmp = this.ex;
                while (true)
                {
                    AggregateException aggregate = existingCmp is null
                        ? new(exs)
                        : new([..existingCmp.InnerExceptions, ..exs]);

                    if (object.ReferenceEquals(
                        existingCmp,
                        existingCmp = Interlocked.CompareExchange(ref this.ex, aggregate, existingCmp))
                    )
                        break;
                }
            }

            return;
        }
    }
}