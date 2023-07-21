// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.SQLite
{
    internal class SQLiteSink : BatchProvider, ILogEventSink
    {
        private readonly string _filename;
        private readonly string _ext;
        private readonly string _directory;
        private readonly string _userInputPath;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly uint _maxDatabaseSize;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly TimeSpan? _retentionPeriod;
        private readonly Timer _retentionTimer;
        private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffzzz";
        private const long BytesPerMb = 1_048_576;
        private const long MaxSupportedPages = 5_242_880;
        private const long MaxSupportedPageSize = 4096;
        private const long MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);
        private string PrevFileName { get; set; } = string.Empty;

        public SQLiteSink(
            string sqlLiteDbPath,
            string tableName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            TimeSpan? retentionPeriod,
            TimeSpan? retentionCheckInterval,
            uint batchSize = 100,
            uint maxDatabaseSize = 10,
            bool rollOver = true) : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _userInputPath = sqlLiteDbPath;
            _directory = Path.GetDirectoryName(sqlLiteDbPath);
            _filename = Path.GetFileNameWithoutExtension(sqlLiteDbPath);
            _ext = Path.GetExtension(sqlLiteDbPath);
            _tableName = tableName;
            _formatProvider = formatProvider;
            _storeTimestampInUtc = storeTimestampInUtc;
            _maxDatabaseSize = maxDatabaseSize;
            _rollOver = rollOver;

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                throw new SQLiteException($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            InitializeDatabase();

            if (retentionPeriod.HasValue)
            {
                // impose a min retention period of 15 minute
                var retentionCheckMinutes = 15;
                if (retentionCheckInterval.HasValue)
                {
                    retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
                }

                // impose multiple of 15 minute interval
                retentionCheckMinutes = (retentionCheckMinutes / 15) * 15;

                _retentionPeriod = new[] { retentionPeriod, TimeSpan.FromMinutes(30) }.Max();

                // check for retention at this interval - or use retentionPeriod if not specified
                _retentionTimer = new Timer(
                    (x) => { ApplyRetentionPolicy(); },
                    null,
                    TimeSpan.FromMinutes(0),
                    TimeSpan.FromMinutes(retentionCheckMinutes));
            }
        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            try
            {
                using (var conn = GetSqLiteConnection())
                {
                    CreateSqlTable(conn);
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine($"Error: {ex}");
                throw;
            }
        }

        private string GetActiveDbPath()
        {
            return Path.Combine(_directory ?? "Logs", $"{_filename}-{DateTime.Now:yyyyMMdd}{_ext}");
        }

        private SQLiteConnection GetSqLiteConnection()
        {
            var dbPath = GetActiveDbPath();
            if (string.IsNullOrEmpty(PrevFileName))
            {
                PrevFileName = dbPath;
            }
            bool isDbExists = File.Exists(dbPath);
            if (!isDbExists)
            {
                SelfLog.WriteLine($"Rolling database to {dbPath}");
                var prev_sqlConString = new SQLiteConnectionStringBuilder
                {
                    DataSource = PrevFileName,
                    JournalMode = SQLiteJournalModeEnum.Wal,
                    SyncMode = SynchronizationModes.Normal,
                    CacheSize = 500,
                    PageSize = (int)MaxSupportedPageSize,
                    MaxPageCount = (int)(_maxDatabaseSize * BytesPerMb / MaxSupportedPageSize),
                }.ConnectionString;
                var prev_sqLiteConnection = new SQLiteConnection(prev_sqlConString, true);
                prev_sqLiteConnection.Open();
                prev_sqLiteConnection.Close();
                prev_sqLiteConnection.Dispose();

                PrevFileName = dbPath;
            }
            var sqlConString = new SQLiteConnectionStringBuilder
            {
                //DataSource = _databasePath,
                DataSource = dbPath,
                JournalMode = SQLiteJournalModeEnum.Wal,
                SyncMode = SynchronizationModes.Normal,
                CacheSize = 500,
                PageSize = (int)MaxSupportedPageSize,
                MaxPageCount = (int)(_maxDatabaseSize * BytesPerMb / MaxSupportedPageSize),
            }.ConnectionString;

            var sqLiteConnection = new SQLiteConnection(sqlConString, true);
            sqLiteConnection.Open();
            if (!isDbExists)
            {
                CreateSqlTable(sqLiteConnection);
            }
            return sqLiteConnection;
        }

        private void CreateSqlTable(SQLiteConnection sqlConnection)
        {
            var colDefs = "id INTEGER PRIMARY KEY AUTOINCREMENT,";
            colDefs += "Timestamp TEXT,";
            colDefs += "Level VARCHAR(10),";
            colDefs += "ThreadId INTEGER,";
            colDefs += "RenderedMessage TEXT,";
            colDefs += "Exception TEXT,";
            colDefs += "SourceContext TEXT,";
            colDefs += "MachineName TEXT,";
            colDefs += "MemoryUsage BIGINT,";
            colDefs += "Properties TEXT";

            var sqlCreateText = $"CREATE TABLE IF NOT EXISTS {_tableName} ({colDefs})";

            using (var sqlCommand = new SQLiteCommand(sqlCreateText, sqlConnection))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }

        private SQLiteCommand CreateSqlInsertCommand(SQLiteConnection connection)
        {
            var sqlInsertText = "INSERT INTO {0} (Timestamp, Level, Exception, RenderedMessage, Properties, ThreadId, MachineName, MemoryUsage, SourceContext)";
            sqlInsertText += " VALUES (@timeStamp, @level, @exception, @renderedMessage, @properties, @threadid, @machinename, @memoryusage, @sourcecontext)";
            sqlInsertText = string.Format(sqlInsertText, _tableName);

            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInsertText;
            sqlCommand.CommandType = CommandType.Text;

            sqlCommand.Parameters.Add(new SQLiteParameter("@timeStamp", DbType.DateTime2));
            sqlCommand.Parameters.Add(new SQLiteParameter("@level", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@exception", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@renderedMessage", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@properties", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@threadid", DbType.Int16));
            sqlCommand.Parameters.Add(new SQLiteParameter("@machinename", DbType.String));
            sqlCommand.Parameters.Add(new SQLiteParameter("@memoryusage", DbType.Int64));
            sqlCommand.Parameters.Add(new SQLiteParameter("@sourcecontext", DbType.String));

            return sqlCommand;
        }

        private void ApplyRetentionPolicy()
        {
            var epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);
            using (var sqlConnection = GetSqLiteConnection())
            {
                using (var cmd = CreateSqlDeleteCommand(sqlConnection, epoch))
                {
                    SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
                    var ret = cmd.ExecuteNonQuery();
                    SelfLog.WriteLine($"{ret} records deleted");
                }
            }
        }

        private void TruncateLog(SQLiteConnection sqlConnection)
        {
            using (var cmd = sqlConnection.CreateCommand())
            {
                cmd.CommandText = $"DELETE FROM {_tableName}";
                cmd.ExecuteNonQuery();
            }
            VacuumDatabase(sqlConnection);
        }

        private void VacuumDatabase(SQLiteConnection sqlConnection)
        {
            using (var cmd = sqlConnection.CreateCommand())
            {
                cmd.CommandText = $"vacuum";
                cmd.ExecuteNonQuery();
            }
        }

        private SQLiteCommand CreateSqlDeleteCommand(SQLiteConnection sqlConnection, DateTimeOffset epoch)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE Timestamp < @epoch";
            cmd.Parameters.Add(
                new SQLiteParameter("@epoch", DbType.DateTime2)
                {
                    Value = (_storeTimestampInUtc ? epoch.ToUniversalTime() : epoch).ToString(
                        TimestampFormat)
                });

            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                using (var sqlConnection = GetSqLiteConnection())
                {
                    try
                    {
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);
                        return true;
                    }
                    catch (SQLiteException e)
                    {
                        SelfLog.WriteLine(e.Message);

                        if (e.ResultCode != SQLiteErrorCode.Full)
                            return false;

                        if (_rollOver == false)
                        {
                            SelfLog.WriteLine("Discarding log excessive of max database");

                            return true;
                        }

                        var newFilePath = Path.Combine(_directory ?? "Logs", $"{_filename}-{DateTime.Now:yyyyMMdd_HHmmss_fff}{_ext}");

                        File.Copy(GetActiveDbPath(), newFilePath, true);

                        TruncateLog(sqlConnection);
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        SelfLog.WriteLine($"Rolling database to {newFilePath}");
                        return true;
                    }
                    catch (Exception e)
                    {
                        SelfLog.WriteLine(e.Message);
                        return false;
                    }
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(ICollection<LogEvent> logEventsBatch, SQLiteConnection sqlConnection)
        {
            try
            {
                using (var tr = sqlConnection.BeginTransaction())
                {
                    using (var sqlCommand = CreateSqlInsertCommand(sqlConnection))
                    {
                        sqlCommand.Transaction = tr;

                        foreach (var logEvent in logEventsBatch)
                        {
                            sqlCommand.Parameters["@timeStamp"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString(TimestampFormat)
                                : logEvent.Timestamp.ToString(TimestampFormat);
                            sqlCommand.Parameters["@level"].Value = logEvent.Level.ToString();
                            if (logEvent.Properties.ContainsKey("ThreadId") && int.TryParse(logEvent.Properties["ThreadId"].ToString(), out int ThreadId))
                            {
                                sqlCommand.Parameters["@threadid"].Value = ThreadId;
                            }
                            sqlCommand.Parameters["@exception"].Value = logEvent.Exception?.ToString();
                            sqlCommand.Parameters["@renderedMessage"].Value = logEvent.MessageTemplate.Render(logEvent.Properties, _formatProvider);

                            if (logEvent.Properties.ContainsKey("MachineName"))
                            {
                                sqlCommand.Parameters["@machinename"].Value = logEvent.Properties["MachineName"]?.ToString();
                            }
                            if (logEvent.Properties.ContainsKey("MemoryUsage") && int.TryParse(logEvent.Properties["MemoryUsage"].ToString(), out int MemoryUsage))
                            {
                                sqlCommand.Parameters["@memoryusage"].Value = MemoryUsage;
                            }
                            if (logEvent.Properties.ContainsKey("SourceContext"))
                            {
                                sqlCommand.Parameters["@sourcecontext"].Value = logEvent.Properties["SourceContext"]?.ToString();
                            }
                            sqlCommand.Parameters["@properties"].Value = logEvent.Properties.Count > 0
                                ? logEvent.Properties.Json()
                                : string.Empty;

                            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                        tr.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine($"Error: {ex}");
                throw;
            }
        }
    }
}
