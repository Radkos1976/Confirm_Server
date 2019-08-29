using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB_Conect
{
    public class Prod_Calendar : Update_pstgr_from_Ora<Prod_Calendar.Calendar>
    {
        /// <summary>
        /// Update table with Calendar Days
        /// </summary>
        /// <returns></returns>
        public async Task<int> Update_calendar_table()
        {
            try
            {
                Update_pstgr_from_Ora<Calendar> rw = new Update_pstgr_from_Ora<Calendar>();
                List<Calendar> list_ora = new List<Calendar>();
                List<Calendar> list_pstgr = new List<Calendar>();
                Parallel.Invoke(async () =>
                {
                    list_ora = await rw.Get_Ora("" +
"SELECT calendar_id, counter, to_date(work_day) work_day, day_type, working_time, working_periods, objid, objversion " +
       "FROM ifsapp.work_time_counter " +
    "WHERE CALENDAR_ID='SITS' ", "Calendar_ORA");
                    list_ora.Sort();
                }, async () => { list_pstgr = await rw.Get_PSTGR("Select * from work_cal WHERE CALENDAR_ID='SITS' order by counter", "Calendar_Pstgr"); list_pstgr.Sort(); });
                Changes_List<Calendar> tmp = rw.Changes(list_pstgr, list_ora, new[] { "id" }, "id", "id");
                list_ora = null;
                list_pstgr = null;
                return await PSTRG_Changes_to_dataTable(tmp, "work_cal", "id", null, null);
            }
            catch (Exception e)
            {
                Loger.Log("Błąd importu CRP:" + e);
                return 1;
            }
        }
        public class Calendar : IEquatable<Calendar>, IComparable<Calendar>
        {
            public string Calendar_id { get; set; }
            public int Counter { get; set; }
            public DateTime Work_day { get; set; }
            public string Day_type { get; set; }
            public double Working_time { get; set; }
            public int Working_peirods { get; set; }
            public string Objid { get; set; }
            public string Objversion { get; set; }
            public int CompareTo(Calendar other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int result = Calendar_id.CompareTo(other.Calendar_id);
                    if (result == 0)
                        result =Counter.CompareTo(other.Counter);
                    return result;
                }
            }
            public bool Equals(Calendar other)
            {
                if (other == null) return false;
                return (Calendar_id.Equals(other.Calendar_id) && Counter.Equals(other.Counter));
            }
        }
    }
}
