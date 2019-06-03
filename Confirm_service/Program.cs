using Common;
using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;


namespace Confirm_service
{
    class Program
    {
        private static readonly string _pluginFolder = @"..\..\..\Plugins\";
        private static FileSystemWatcher _pluginWatcher;
        private static Start _app;

        static void Main(string[] args)
        {
            Console.WriteLine("Starting the main application");

            _pluginWatcher = new FileSystemWatcher(_pluginFolder);
            _pluginWatcher.Created += PluginWatcher_FolderUpdated;
            _pluginWatcher.Deleted += PluginWatcher_FolderUpdated;
            _pluginWatcher.EnableRaisingEvents = true;

            _app = new Start(_pluginFolder);

            PrintPluginInfo();


            foreach (var runnable in _app.Plugins)
            {
                runnable.Value.Run();
            }


            Console.ReadLine();
        }

        private static void PrintPluginInfo()
        {
            Console.WriteLine($"{_app.Plugins.Count} plugin(s) loaded..");
            Console.WriteLine("Displaying plugin info...");
            Console.WriteLine();

            foreach (var runnable in _app.Plugins)
            {
                Console.WriteLine("----------------------------------------");
                Console.WriteLine($"Name: {runnable.Metadata.DisplayName}");
                Console.WriteLine($"Description: {runnable.Metadata.Description}");
                Console.WriteLine($"Version: {runnable.Metadata.Version}");             
            }
        }

        private static void PluginWatcher_FolderUpdated(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine();
            Console.WriteLine("====================================");
            Console.WriteLine("Folder changed. Reloading plugins...");
            Console.WriteLine();

            _app.LoadPlugins();

            PrintPluginInfo();
        }
    }
}
