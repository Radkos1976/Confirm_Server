using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.ComponentModel.Composition;

namespace App_Logic
{

    public class Conf_serv
    {
        private bool isDisposed = false;
        protected void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                if (disposing)
                {

                }
            }
            this.isDisposed = true;
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
        [Import(typeof(Common.IDB_Loger))]
        public Common.IDB_Loger Loger;
        /// <summary>
        /// Start calculations
        /// </summary>
        public void Run()
        {
            Loger.Srv_start();
            start = Loger.Serw_run;
            int main = 0;
            int run_cust = 0;
            int wy = 0;
            try
            {
                Parallel.Invoke(srv_op, async () => run_cust = await Needs_path(), async () => main = await Supply_path());
                wy = run_cust + main;
            }
            catch (AggregateException e)
            {
                Loger.Log("Błąd wywołania asynchronicznego :" + e);
                wy = 2;
            }
            wy = run_cust + main;
        }
    }
}
