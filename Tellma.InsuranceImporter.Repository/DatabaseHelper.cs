using System.Data;
using System.Data.OleDb;

namespace Tellma.InsuranceImporter.Repository
{
    public class DatabaseHelper : IDisposable
    {
        private OleDbConnection _connection;
        private readonly string _connectionString;

        public DatabaseHelper(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public async Task<OleDbConnection> GetOpenConnectionAsync()
        {
            _connection = new OleDbConnection(_connectionString);

            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync();
            }

            return _connection;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _connection.Dispose();
            }
        }
    }
}
