using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DB_Conect
{
    [Export(typeof(IDbOperations))]
    class DB : IDbOperations
    {
        public void Update_cust_ord()
        {
            int a = 0;
            Cust_orders get = new Cust_orders();
            Parallel.Invoke(async () => { 
                if (! get.Updated_on_init) 
                { 
                    a = await get.Update_cust(); 
                } 
            });           
        }

    }
}
