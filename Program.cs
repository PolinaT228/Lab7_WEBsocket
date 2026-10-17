using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

class TaskItem
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public bool Completed { get; set; }
}

class RequestMessage
{
    public string Action { get; set; } = string.Empty;
    public string? Title { get; set; }
    public int Id { get; set; }
}

class Program
{
    private static ConcurrentBag<TaskItem> tasks = new ConcurrentBag<TaskItem>
    {
        new TaskItem { Id = 1, Title = "Вивчити протокол WebSocket", Completed = false },
        new TaskItem { Id = 2, Title = "Здати лабораторну роботу №7", Completed = false }
    };
    private static int nextId = 3;

    private static ConcurrentDictionary<string, WebSocket> clients = new ConcurrentDictionary<string, WebSocket>();

    static async Task Main(string[] args)
    {
        string url = "http://localhost:5000/";
        HttpListener listener = new HttpListener();
        listener.Prefixes.Add(url);
        listener.Start();
        Console.WriteLine($"[Сервер] Запущено на {url}");

        while (true)
        {
            var context = await listener.GetContextAsync();
            if (context.Request.IsWebSocketRequest)
            {
                _ = Task.Run(() => HandleWebSocketAsync(context));
            }
            else
            {
                context.Response.StatusCode = 400;
                context.Response.Close();
            }
        }
    }

    private static async Task HandleWebSocketAsync(HttpListenerContext context)
    {
        var webSocketContext = await context.AcceptWebSocketAsync(null);
        var socket = webSocketContext.WebSocket;
        string clientId = Guid.NewGuid().ToString();
        clients.TryAdd(clientId, socket);
        Console.WriteLine($"[Сервер] Клієнт підключився: {clientId}");

        var buffer = new byte[1024 * 4];
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                // Читаємо повідомлення у правильному кодуванні UTF-8
                string messageJson = Encoding.UTF8.GetString(buffer, 0, result.Count);
                var request = JsonSerializer.Deserialize<RequestMessage>(messageJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                if (request != null)
                {
                    ProcessAction(request);
                    await BroadcastTasksAsync();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Помилка] {ex.Message}");
        }
        finally
        {
            clients.TryRemove(clientId, out _);
            if (socket.State == WebSocketState.Open || socket.State == WebSocketState.CloseReceived)
            {
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by server", CancellationToken.None);
            }
            Console.WriteLine($"[Сервер] Клієнт відключився: {clientId}");
        }
    }

    private static void ProcessAction(RequestMessage request)
    {
        switch (request.Action)
        {
            case "add_task":
                if (!string.IsNullOrWhiteSpace(request.Title))
                {
                    var newTask = new TaskItem 
                    { 
                        Id = Interlocked.Increment(ref nextId) - 1, 
                        Title = request.Title, 
                        Completed = false 
                    };
                    tasks.Add(newTask);
                }
                break;

            case "toggle_task":
                foreach (var task in tasks)
                {
                    if (task.Id == request.Id)
                    {
                        task.Completed = !task.Completed;
                    }
                }
                break;
        }
    }

    private static async Task BroadcastTasksAsync()
    {
        var responseObj = new
        {
            status = "success",
            tasks = tasks.OrderBy(t => t.Id).Select(t => new { id = t.Id, title = t.Title, completed = t.Completed })
        };

        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        string jsonResponse = JsonSerializer.Serialize(responseObj, options);
        var bytes = Encoding.UTF8.GetBytes(jsonResponse);

        foreach (var client in clients.Values)
        {
            if (client.State == WebSocketState.Open)
            {
                await client.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
            }
        }
    }
}