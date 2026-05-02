using System.Collections.Concurrent;
using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Synchronous ADO entry points (Open / ExecuteNonQuery / ExecuteScalar / Read /
/// Close) wrap async work via <c>GetAwaiter().GetResult()</c>. If any await in
/// the underlying chain captures a single-threaded <see cref="SynchronizationContext"/>
/// (legacy ASP.NET, WPF, WinForms), the resumption is queued back to the caller
/// thread which is blocked on <c>GetResult()</c> — classic async-over-sync
/// deadlock. The contract this test pins: the sync entry points must not
/// deadlock under a single-threaded sync context.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class SyncOnSyncContextDeadlockTests
{
    private readonly SingleNodeFixture _fx;

    public SyncOnSyncContextDeadlockTests(SingleNodeFixture fx) => _fx = fx;

    [Fact]
    public void Open_ExecuteScalar_Read_Close_DoNotDeadlock_UnderSingleThreadContext()
    {
        var ctx = new SingleThreadedSyncContext();
        SynchronizationContext? previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(ctx);
        try
        {
            using var conn = new ClickHouseDbConnection(_fx.ConnectionString);

            // Post the entire sync flow to the single-threaded pump. If any await
            // inside the chain captures this context, the resumption queues
            // behind the posted callback that's already blocked on
            // GetResult() — classic deadlock.
            var done = new ManualResetEventSlim();
            Exception? failure = null;
            ctx.Post(_ =>
            {
                try
                {
                    conn.Open();

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT 42";
                    var scalar = cmd.ExecuteScalar();
                    Assert.Equal(42, Convert.ToInt32(scalar));

                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.True(reader.Read());
                        Assert.Equal(42, reader.GetInt32(0));
                        Assert.False(reader.Read());
                    }

                    conn.Close();
                }
                catch (Exception ex)
                {
                    failure = ex;
                }
                finally
                {
                    done.Set();
                }
            }, null);

            Assert.True(done.Wait(TimeSpan.FromSeconds(8)),
                "Sync ADO call deadlocked under SingleThreadedSyncContext.");
            if (failure is not null) throw failure;
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    private sealed class SingleThreadedSyncContext : SynchronizationContext
    {
        private readonly BlockingCollection<(SendOrPostCallback cb, object? state)> _queue = new();

        public SingleThreadedSyncContext()
        {
            var pumpThread = new Thread(Pump) { IsBackground = true, Name = "SingleThreadedSyncContext" };
            pumpThread.Start();
        }

        private void Pump()
        {
            foreach (var (cb, state) in _queue.GetConsumingEnumerable())
            {
                try { cb(state); } catch { /* swallow to keep pump alive */ }
            }
        }

        public override void Post(SendOrPostCallback d, object? state) => _queue.Add((d, state));

        public override void Send(SendOrPostCallback d, object? state)
        {
            using var done = new ManualResetEventSlim();
            Post(_ =>
            {
                try { d(state); } finally { done.Set(); }
            }, null);
            done.Wait();
        }
    }
}
