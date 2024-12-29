using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Schema;
using Durable.Function.TxDataPatch.Validation.OperationHandler.Table;
using Durable.Function.TxDataPatch.Validation.OperationHandler.Container;
using Durable.Function.TxDataPatch.Validation.Config;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddSingleton<AppConfig>();
        services.AddSingleton<JSchema>(provider =>
        {
            var schemaFilePath = GetSchemaFilePath();
            var schemaContent = File.ReadAllText(schemaFilePath);
            return JSchema.Parse(schemaContent);
        });
        services.AddSingleton<ITableStorageManager, TableStorageManager>();
        services.AddSingleton<IBlobStorageManager, BlobStorageManager>();
    })
    .Build();

await host.RunAsync();

static string GetSchemaFilePath()
{
    var codeBaseUri = new Uri(Assembly.GetExecutingAssembly().Location);
    var codeBasePath = Uri.UnescapeDataString(codeBaseUri.AbsolutePath);
    var dirPath = Path.GetDirectoryName(codeBasePath);

    var projectDirectory = SearchForProjectDirectory(dirPath);
    return Path.Combine(projectDirectory, "Templates", "json_Schema_global_Customer_Inbound_v7.4.1_r3.1.json");
}

static string SearchForProjectDirectory(string currentDirectory)
{
    while (true)
    {
        var projectFiles = Directory.GetFiles(currentDirectory, "*.csproj");
        if (projectFiles.Any())
        {
            return currentDirectory;
        }

        var parentDirectory = Directory.GetParent(currentDirectory);
        if (parentDirectory == null)
        {
            throw new Exception("Project directory not found.");
        }
        currentDirectory = parentDirectory.FullName;
    }
}
