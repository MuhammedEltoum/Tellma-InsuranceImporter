using MailKit.Net.Smtp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MimeKit;
using System.Text;
using System.Threading;
using Google.GenAI;
using Google.GenAI.Types;

namespace Tellma.Utilities.EmailLogger
{
    public class EmailLogger : ILogger
    {
        private readonly EmailOptions _options;
        private readonly IEnumerable<string> _emails;
        private readonly IEnumerable<string> _reportEmails;
        private readonly List<LogEntry> _logEntries = new();
        private readonly object _lock = new object();

        // Retry configuration
        private const int MaxRetryAttempts = 5;
        private readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);
        private readonly Random _random = new Random();

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

                // Send with retry logic
                ExecuteWithRetry(() => SendEmail(message));
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to send log email after all retry attempts.", e);
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
                    var reportHtml = String.IsNullOrWhiteSpace(_options.GoogleGeminiApiKey) ? GenerateReportHtml(reportTitle) : Task.Run(() => GenerateAIDashboardReport()).Result;

                    var bodyBuilder = new BodyBuilder();
                    bodyBuilder.TextBody = reportText;
                    bodyBuilder.HtmlBody = reportHtml;

                    reportMessage.Body = bodyBuilder.ToMessageBody();

                    // Send with retry logic
                    ExecuteWithRetry(() => SendEmail(reportMessage));

                    Console.WriteLine("Email sent successfully!");
                    // Clear log entries after sending report
                    _logEntries.Clear();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to send activity report after all retry attempts: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Executes an email sending operation with retry logic.
        /// </summary>
        private void ExecuteWithRetry(Action sendAction)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    sendAction();
                    return; // Success - exit the retry loop
                }
                catch (Exception ex)
                {
                    attempt++;

                    if (attempt >= MaxRetryAttempts)
                    {
                        throw new InvalidOperationException($"Failed to send email after {MaxRetryAttempts} attempts", ex);
                    }

                    // Log the retry attempt
                    Console.WriteLine($"Email sending attempt {attempt} failed. Retrying in {RetryDelay.TotalSeconds} seconds. Error: {ex.Message}");

                    // Wait before retry with exponential backoff and jitter
                    var delay = CalculateDelay(attempt);
                    Thread.Sleep(delay);
                }
            }
        }

        /// <summary>
        /// Sends a single email message.
        /// </summary>
        private void SendEmail(MimeMessage message)
        {
            using var client = new SmtpClient();

            // Set a timeout for the SMTP operations
            client.Timeout = 30000; // 30 seconds

            try
            {
                // Connect with timeout
                client.Connect(_options.SmtpHost, _options.SmtpPort ?? 587, _options.SmtpUseSsl);

                // Note: only needed if the SMTP server requires authentication
                if (!string.IsNullOrWhiteSpace(_options.SmtpUsername))
                    client.Authenticate(_options.SmtpUsername, _options.SmtpPassword);

                // Send the message
                client.Send(message);
                client.Disconnect(true);
            }
            catch
            {
                // Ensure client is disconnected on failure
                if (client.IsConnected)
                {
                    try { client.Disconnect(true); } catch { }
                }
                throw;
            }
        }

        /// <summary>
        /// Calculates delay with exponential backoff and jitter.
        /// </summary>
        private TimeSpan CalculateDelay(int attempt)
        {
            // Exponential backoff: 2^attempt * base delay
            double exponentialDelay = Math.Pow(2, attempt - 1) * RetryDelay.TotalSeconds;

            // Add jitter (±25%) to avoid thundering herd
            double jitter = (_random.NextDouble() * 0.5 - 0.25) * exponentialDelay;
            double totalDelay = exponentialDelay + jitter;

            // Cap at 30 seconds maximum delay
            totalDelay = Math.Min(totalDelay, 30);

            return TimeSpan.FromSeconds(totalDelay);
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
    </div>
<br>

");

            foreach (var entry in CleanLogEntries(_logEntries))
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

        private List<LogEntry> CleanLogEntries(List<LogEntry> logEntries)
        {
            string[] excludePrefix = new string[]
            {
                "Application started",
                "Hosting environment:",
                "Content root path: "
            };

            return logEntries
                .Where(le => !excludePrefix.Any(prefix => le.Message.StartsWith(prefix)))
                .ToList();
        }

        private async Task<string> GenerateAIDashboardReport()
        {
            if (string.IsNullOrWhiteSpace(_options.GoogleGeminiApiKey))
            {
                return string.Empty;
            }

            // Generate report with retry logic
            return await ExecuteWithRetryAsync(async () =>
            {
                var sb = new StringBuilder();
                foreach (var entry in CleanLogEntries(_logEntries))
                {
                    sb.AppendLine($"{entry.Timestamp:HH:mm:ss} [{entry.Level}] {entry.Message}");
                }

                var client = new Client(apiKey: _options.GoogleGeminiApiKey);

                string prompt = $@"
                            Role: You are a Frontend Email Automation Bot.

Task: I will provide you with Server Logs. You must output a single HTML file based strictly on the HTML Template provided below.

Instructions:

Analyze the Logs: Calculate the total time, identify the status of the 4 sections (Exchange, Remittance, Technical, Pairing), and summarize the specific errors/warnings. Keep the summary and messages concise and professional and of similar structure for all Remittance, Technical, and Pairing.

Fill the Template: Replace the content inside the HTML tags with the data from the logs.

Visual Logic:

If a section has no errors, use the class badge-success and text ""Success"".

If a section is empty/skipped, use the class badge-warning and text ""Empty"" or ""Skipped"".

If a section has errors, use the class badge-error and text ""Error"" or ""Fail"".

Raw Logs: detailed logs must be placed inside the <pre> tag at the bottom of the email.

Do NOT change the CSS or Layout. Only update the text content and the specific classes mentioned above.

The HTML Template:

HTML

<!DOCTYPE html>
<html lang=""en"">
<head>
    <meta charset=""UTF-8"">
    <meta name=""viewport"" content=""width=device-width, initial-scale=1.0"">
    <title>Import Status Report</title>
    <style>
        body {{ margin: 0; padding: 0; background-color: #f4f6f8; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; -webkit-font-smoothing: antialiased; }}
        .wrapper {{ width: 100%; table-layout: fixed; background-color: #f4f6f8; padding-bottom: 40px; }}
        .main-container {{ background-color: #ffffff; margin: 0 auto; width: 100%; max-width: 600px; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); margin-top: 20px; }}
        .header {{ background-color: #2c3e50; padding: 30px 40px; text-align: center; }}
        .header h1 {{ color: #ffffff; margin: 0; font-size: 24px; font-weight: 600; }}
        .header p {{ color: #aab7c4; margin: 5px 0 0 0; font-size: 14px; }}
        .summary-grid {{ padding: 20px 40px; background-color: #ecf0f1; border-bottom: 1px solid #e1e8ed; }}
        .stat-box {{ width: 32%; display: inline-block; vertical-align: top; text-align: center; }}
        .stat-label {{ font-size: 11px; text-transform: uppercase; color: #7f8c8d; letter-spacing: 1px; font-weight: bold; }}
        .stat-value {{ font-size: 18px; font-weight: 700; color: #2c3e50; margin-top: 5px; }}
        .content-section {{ padding: 30px 40px; border-bottom: 1px solid #f0f0f0; }}
        .section-title {{ font-size: 18px; font-weight: 600; color: #34495e; margin: 0; }}
        .badge {{ padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: bold; text-transform: uppercase; }}
        .badge-success {{ background-color: #e8f8f5; color: #27ae60; }}
        .badge-warning {{ background-color: #fef9e7; color: #f1c40f; }}
        .badge-error {{ background-color: #fdedec; color: #c0392b; }}
        .issue-list {{ background-color: #fff8f8; border-left: 4px solid #e74c3c; padding: 15px; margin-top: 15px; border-radius: 4px; }}
        .issue-list.warning {{ background-color: #fffcf0; border-left: 4px solid #f1c40f; }}
        .issue-item {{ font-size: 13px; color: #555; margin-bottom: 8px; line-height: 1.4; display: block; }}
        .raw-log-container {{ background-color: #1e1e1e; color: #cccccc; padding: 20px; font-family: 'Courier New', Courier, monospace; font-size: 11px; overflow-x: hidden; white-space: pre-wrap; word-wrap: break-word; }}
        .footer {{ background-color: #f4f6f8; padding: 20px; text-align: center; font-size: 12px; color: #95a5a6; }}
    </style>
</head>
<body>
    <div class=""wrapper"">
        <div class=""main-container"">
            <div class=""header"">
                <h1>Import Status Report</h1>
                <p>Tenant: [INSERT TENANT NAME/ID HERE]</p>
            </div>

            <div class=""summary-grid"">
                <div class=""stat-box"">
                    <div class=""stat-label"">Total Time</div>
                    <div class=""stat-value"">[INSERT TOTAL TIME]</div>
                </div>
                <div class=""stat-box"">
                    <div class=""stat-label"">Date</div>
                    <div class=""stat-value"">{DateTime.Now:f}</div>
                </div>
                <div class=""stat-box"">
                    <div class=""stat-label"">Overall Status</div>
                    <div class=""stat-value"" style=""color: #2c3e50;"">[INSERT OVERALL STATUS]</div>
                </div>
            </div>

            <div class=""content-section"">
                <div style=""margin-bottom: 10px;"">
                    <span class=""section-title"">1. Exchange Rates</span>
                    <span class=""badge [INSERT CLASS]"" style=""float: right;"">[INSERT STATUS TEXT]</span>
                </div>
                <div style=""font-size: 13px; color: #555;"">
                    [INSERT 1 SENTENCE SUMMARY OR SUCCESS MESSAGE]
                </div>
            </div>

            <div class=""content-section"">
                <div style=""margin-bottom: 10px;"">
                    <span class=""section-title"">2. Remittance</span>
                    <span class=""badge [INSERT CLASS]"" style=""float: right;"">[INSERT STATUS TEXT]</span>
                </div>
                <div style=""font-size: 13px; color: #555;"">
                     [INSERT SUMMARY]
                </div>
                 <div class=""issue-list [ADD CLASS]"">
                    <div class=""issue-item"">[INSERT SUMMARY]</div>
                    <div class=""issue-item"">[DETAILS]</div>
                </div>
            </div>

            <div class=""content-section"">
                <div style=""margin-bottom: 10px;"">
                    <span class=""section-title"">3. Technical Data</span>
                    <span class=""badge [INSERT CLASS]"" style=""float: right;"">[INSERT STATUS TEXT]</span>
                </div>
                <div style=""font-size: 13px; color: #555;"">
                     [INSERT SUMMARY]
                </div>
                 <div class=""issue-list [ADD CLASS]"">
                    <div class=""issue-item"">[INSERT SUMMARY]</div>
                    <div class=""issue-item"">[DETAILS]</div>
                </div>
            </div>

            <div class=""content-section"">
                <div style=""margin-bottom: 10px;"">
                    <span class=""section-title"">4. Pairing</span>
                    <span class=""badge [INSERT CLASS]"" style=""float: right;"">[INSERT STATUS TEXT]</span>
                </div>
                 <div class=""issue-list [ADD CLASS]"">
                    <div class=""issue-item"">[INSERT SUMMARY]</div>
                    <div class=""issue-item"">[DETAILS]</div>
                </div>
            </div>

            <div class=""raw-log-container"">
                <h3 style=""color: #fff; margin-top: 0; border-bottom: 1px solid #444; padding-bottom: 10px;"">Raw System Logs</h3>
<pre>[INSERT FULL RAW LOGS TEXT HERE]</pre>
            </div>

            <div class=""footer"">
                <p>Generated by Tellma Insurance Importer</p>
            </div>
        </div>
    </div>
</body>
</html>
Logs: {sb.ToString()}";

                var response = await client.Models.GenerateContentAsync(
                    model: _options.Model ?? "gemini-2.5-flash",
                    contents: prompt
                );

                var candidate = response?.Candidates?.FirstOrDefault();

                if (candidate?.Content?.Parts != null)
                {
                    var resultText = string.Join("\n",
                        candidate.Content.Parts.Select(p => p.Text));

                    resultText = resultText
                        .Replace("```html", "")
                        .Replace("```", "")
                        .Trim();

                    return !string.IsNullOrWhiteSpace(resultText)
                        ? resultText
                        : GenerateReportHtml("Service Activity Report");
                }

                return GenerateReportHtml("Service Activity Report");
            });
        }

        /// <summary>
        /// Executes an async operation with retry logic.
        /// </summary>
        private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    return await operation();
                }
                catch (Exception ex)
                {
                    attempt++;

                    if (attempt >= MaxRetryAttempts)
                    {
                        throw new InvalidOperationException($"Operation failed after {MaxRetryAttempts} attempts", ex);
                    }

                    Console.WriteLine($"Operation attempt {attempt} failed. Retrying in {RetryDelay.TotalSeconds} seconds. Error: {ex.Message}");

                    var delay = CalculateDelay(attempt);
                    await Task.Delay(delay);
                }
            }
        }

        public void ClearLogs()
        {
            _logEntries.Clear();
        }
    }
}