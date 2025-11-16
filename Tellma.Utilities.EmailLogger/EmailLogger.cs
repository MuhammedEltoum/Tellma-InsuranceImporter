using MailKit.Net.Smtp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MimeKit;
using System.Text;
using System.Threading;

namespace Tellma.Utilities.EmailLogger
{
    public class EmailLogger : ILogger
    {
        private readonly EmailOptions _options;
        private readonly IEnumerable<string> _emails;
        private readonly IEnumerable<string> _reportEmails;
        private readonly List<LogEntry> _logEntries = new();
        private readonly object _lock = new object();

        public EmailLogger(IOptions<EmailOptions> options)
        {
            _options = options.Value;
            _emails = (_options.EmailAddresses ?? "").Split(",").Select(s => s.Trim()).ToList();
            _reportEmails = (_options.ReportEmailAddresses ?? "").Split(",").Select(s => s.Trim()).ToList();
        }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return (_emails.Any() && (logLevel == LogLevel.Error || logLevel == LogLevel.Critical)) ||
               (_reportEmails.Any() && _options.EnableDetailedReports);
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            // Store log entries for detailed reports
            if (_options.EnableDetailedReports && _reportEmails.Any())
            {
                var logEntry = new LogEntry
                {
                    Timestamp = DateTime.Now,
                    Level = logLevel,
                    Message = formatter(state, exception),
                    Exception = exception
                };

                lock (_lock)
                {
                    _logEntries.Add(logEntry);
                }
            }

            if (exception == null) return;
            if (!IsEnabled(logLevel)) return;
            try
            {
                var message = new MimeMessage();
                message.From.Add(new MailboxAddress("Email Logger", "donotreply@tellma.com"));
                foreach (var email in _emails)
                    message.To.Add(new MailboxAddress(email, email));
                message.Subject = $"{_options.InstallationIdentifier ?? "Unknown"}: Unhandled {exception.GetType().Name}: {Truncate(exception.Message, 50, true)}";

                message.Body = new TextPart("plain")
                {
                    Text = $@"
{formatter(state, exception)}

--- Stack Trace ---

{exception}"
                };

                using var client = new SmtpClient();
                client.Connect(_options.SmtpHost, _options.SmtpPort ?? 587, _options.SmtpUseSsl);

                // Note: only needed if the SMTP server requires authentication
                if (!string.IsNullOrWhiteSpace(_options.SmtpUsername))
                    client.Authenticate(_options.SmtpUsername, _options.SmtpPassword);

                client.Send(message);
                client.Disconnect(true);
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to send log email.", e);
            }
        }

        /// <summary>
        /// Removes all characters after a certain length.
        /// </summary>

        // Method to send detailed activity report
        public void SendActivityReport(string reportTitle = "Service Activity Report")
        {
            if (!_reportEmails.Any()) return;

            lock (_lock)
            {
                if (!_logEntries.Any()) return;

                try
                {
                    var reportMessage = new MimeMessage();
                    reportMessage.From.Add(new MailboxAddress("Service Reporter", "donotreply@tellma.com"));
                    foreach (var email in _reportEmails)
                        reportMessage.To.Add(new MailboxAddress(email, email));

                    reportMessage.Subject = $"{_options.InstallationIdentifier ?? "Unknown"}: {reportTitle} - {DateTime.Now:yyyy-MM-dd HH:mm:ss}";

                    var reportText = GenerateReportText();
                    var reportHtml = GenerateReportHtml(reportTitle);

                    var bodyBuilder = new BodyBuilder();
                    bodyBuilder.TextBody = reportText;
                    bodyBuilder.HtmlBody = reportHtml;

                    reportMessage.Body = bodyBuilder.ToMessageBody();
                    
                    using var client = new SmtpClient();
                    client.Connect(_options.SmtpHost, _options.SmtpPort ?? 587, _options.SmtpUseSsl);

                    if (!string.IsNullOrWhiteSpace(_options.SmtpUsername))
                        client.Authenticate(_options.SmtpUsername, _options.SmtpPassword);

                    client.Send(reportMessage);
                    client.Disconnect(true);

                    // Clear log entries after sending report
                    _logEntries.Clear();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to send activity report: {ex.Message}");
                }
            }
        }

        private string GenerateReportText()
        {
            var report = new StringBuilder();
            report.AppendLine($"Service Activity Report");
            report.AppendLine($"Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            report.AppendLine($"Installation: {_options.InstallationIdentifier ?? "Unknown"}");
            report.AppendLine();
            report.AppendLine("Log Entries:");
            report.AppendLine(new string('-', 80));

            foreach (var entry in _logEntries)
            {
                report.AppendLine($"{entry.Timestamp:HH:mm:ss} [{entry.Level}] {entry.Message}");
                if (entry.Exception != null)
                {
                    report.AppendLine($"Exception: {entry.Exception.Message}");
                }
                report.AppendLine();
            }

            report.AppendLine($"Total entries: {_logEntries.Count}");
            return report.ToString();
        }

        private string GenerateReportHtml(string title)
        {
            var html = new StringBuilder();
            html.AppendLine($@"<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; }}
        .log-entry {{ margin: 10px 0; padding: 10px; border-left: 4px solid #007bff; }}
        .error {{ border-left-color: #dc3545; background-color: #f8d7da; }}
        .warning {{ border-left-color: #ffc107; background-color: #fff3cd; }}
        .info {{ border-left-color: #17a2b8; background-color: #d1ecf1; }}
        .timestamp {{ color: #6c757d; font-size: 0.9em; }}
        .exception {{ background-color: #f8f9fa; padding: 10px; margin: 5px 0; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class='header'>
        <h1>{title}</h1>
        <p><strong>Generated:</strong> {DateTime.Now:yyyy-MM-dd HH:mm:ss}</p>
        <p><strong>Installation:</strong> {_options.InstallationIdentifier ?? "Unknown"}</p>
        <p><strong>Total Entries:</strong> {_logEntries.Count}</p>
    </div>");

            foreach (var entry in _logEntries)
            {
                var levelClass = entry.Level switch
                {
                    LogLevel.Error or LogLevel.Critical => "error",
                    LogLevel.Warning => "warning",
                    _ => "info"
                };

                html.AppendLine($@"
    <div class='log-entry {levelClass}'>
        <div class='timestamp'>{entry.Timestamp:HH:mm:ss} [{entry.Level}]</div>
        <div>{entry.Message}</div>");

                if (entry.Exception != null)
                {
                    html.AppendLine($@"
        <div class='exception'>
            <strong>Exception:</strong> {entry.Exception.Message}<br>
            <pre>{entry.Exception}</pre>
        </div>");
                }

                html.AppendLine("</div>");
            }

            html.AppendLine("</body></html>");
            return html.ToString();
        }

        public static string Truncate(string value, int maxLength, bool appendEllipses = false)
        {
            const string ellipses = "...";

            if (maxLength < 0)
            {
                return value;
            }
            else if (string.IsNullOrEmpty(value))
            {
                return value;
            }
            else if (value.Length <= maxLength)
            {
                return value;
            }
            else
            {
                var truncated = value.Substring(0, maxLength);
                if (appendEllipses)
                {
                    truncated += ellipses;
                }

                return truncated;
            }
        }
    }
}
