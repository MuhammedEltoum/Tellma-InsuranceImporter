using System.Data;
using System.Data.OleDb;

namespace Tellma.InsuranceImporter.Repository
{
    public abstract class RepositoryBase
    {
        private readonly DatabaseHelper _dbHelper;

        protected RepositoryBase(string connectionString)
        {
            _dbHelper = new DatabaseHelper(connectionString);
        }

        protected async Task<OleDbConnection> GetConnectionAsync()
        {
            return await _dbHelper.GetOpenConnectionAsync();
        }

        protected async Task<int> ExecuteNonQueryAsync(string sql, params OleDbParameter[] parameters)
        {
            using var connection = await GetConnectionAsync();
            using var command = new OleDbCommand(sql, connection);

            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }

            return await command.ExecuteNonQueryAsync();
        }

        protected async Task<object> ExecuteScalarAsync(string sql, params OleDbParameter[] parameters)
        {
            using var connection = await GetConnectionAsync();
            using var command = new OleDbCommand(sql, connection);

            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }

            return await command.ExecuteScalarAsync();
        }

        protected async Task<OleDbDataReader> ExecuteReaderAsync(string sql, params OleDbParameter[] parameters)
        {
            var connection = await GetConnectionAsync();
            var command = new OleDbCommand(sql, connection);

            // Set a longer timeout for potentially long-running queries
            command.CommandTimeout = 300; // 5 minutes

            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }

            // Note: CommandBehavior.CloseConnection ensures the connection is closed when the reader is closed
            return (OleDbDataReader)await command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        }
    }
}
