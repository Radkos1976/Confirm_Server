using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using Common;

namespace App_Logic
{
    [Export(typeof(IRunnable))]
    [ExportMetadata("DisplayName", "Confirmatiion server")]
    [ExportMetadata("Description", "Confirm orders and optimize production ")]
    [ExportMetadata("Version", "0.1")]
    public class Run_serv : IRunnable
    {   

        public void Run()
        {
            using(Conf_serv _Serv=new Conf_serv())
            {                
                _Serv.Run();
            }
        }
    }
    public class Conf_serv :IDisposable 
    {
        private static readonly string _pluginFolder = @"..\..\..\Plugins\";
        private CompositionContainer _container;
        private readonly DirectoryCatalog _catalog;
        public Conf_serv()
        {
            _catalog = new DirectoryCatalog(_pluginFolder);
            _container = new CompositionContainer(_catalog);
            _container.ComposeParts(this);
        }
        private bool isDisposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                if (disposing)
                {
                    _catalog.Dispose();
                    _container.Dispose();
                }
            }
            isDisposed = true;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        static TaskScheduler main_cal = TaskScheduler.Default;
        private readonly ParallelOptions srv_op = new ParallelOptions
        {
            CancellationToken = CancellationToken.None,
            MaxDegreeOfParallelism = 100,
            TaskScheduler = main_cal
        };
        private DateTime start;
        public string serv_state = "Ready";

        [Import(typeof(IDB_Loger))]
        private IDB_Loger Loger { get; set; }

        [Import(typeof(IDbOperations))]
        private IDbOperations DbOperations { get; set; }

        /// <summary>
        /// Start calculations
        /// </summary>        
        public void Run() 
        {
            Loger.Srv_start();
            start = Loger.Serw_run;
           
            try
            {
                // Parallel.Invoke(srv_op, async () => run_cust = await Needs_path(), async () => main = await Supply_path());
               
                DbOperations.Update_cust_ord();                
               
            }
            catch (AggregateException e)
            {
                Loger.Log("Błąd wywołania asynchronicznego :" + e);
               
            }
           
        }
       
    }
}
