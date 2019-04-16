using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;


namespace Confirm_service
{
    class Start
    {
        DirectoryCatalog dirCatalog;
        [Import(typeof(Common.IRunnable),AllowRecomposition =true)]
        Common.IRunnable Serv = null;
        static void Main(string[] args)
        {
            Start p = new Start();
            p.Run();
        }
        public void Run()
        {
            Compose();
            Serv.Run();
        }
        private void Compose()
        {
            //Notice that we are creating two catalogs and adding
            //them to one aggregate catalog
            dirCatalog = new DirectoryCatalog(@"C:\temp");
            AssemblyCatalog assemblyCat = new AssemblyCatalog(System.Reflection.Assembly.GetExecutingAssembly());
            AggregateCatalog catalog = new AggregateCatalog(assemblyCat, dirCatalog);
            CompositionContainer container = new CompositionContainer(catalog);
            container.ComposeParts(this);
        }
    }
}