using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Tellma.Utilities.EmailLogger
{
    public class EmailLoggerProvider : ILoggerProvider
    {
        private readonly EmailLogger _logger;
        public EmailLoggerProvider(EmailLogger logger)
        {
            _logger = logger;
        }
        public ILogger CreateLogger(string categoryName)
        {
            return _logger;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
