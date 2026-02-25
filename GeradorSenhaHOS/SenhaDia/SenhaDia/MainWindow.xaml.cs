using System;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Forms;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media.Imaging;
using MouseEventArgs = System.Windows.Input.MouseEventArgs;

namespace SenhaDia
{
    public partial class MainWindow : Window
    {
        private bool visivel;
        public MainWindow()
        {
            InitializeComponent();
           MouseDown += MainWindow_MouseDown;
            //MouseEnter += MainWindow_MouseEnter;
            MouseLeave += MainWindow_MouseLeave;
            KeyDown += MainWindow_KeyDown;
            PainelAum.MouseEnter += MainWindow_MouseEnter;
            //PainelAum.MouseLeave += MainWindow_MouseLeave;
            image1.MouseEnter += MainWindow_MouseEnter;
           // image1.MouseLeave += MainWindow_MouseLeave;

            WindowStartupLocation = WindowStartupLocation.Manual;
            Left = (Screen.PrimaryScreen.WorkingArea.Width - Width) / 2;
            Top = 0;
            label_Copy1.Text = GerarSenhaDiaria();
            label_Copy2.Text = GeraSenhaRestrita();
            ShowInTaskbar = false;
            
            //   this.Height = 10;
        }

     

        private void MainWindow_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            if (e.Key == Key.Escape)
            {
                Close();
            }
        }

        private void MainWindow_MouseLeave(object sender, MouseEventArgs e)
        {
            if (visivel) return;
            Background.Opacity = 0.5;
            this.Height = 8;
            string bitmapPath = @"Resources/Detalhamento.png";
            BitmapImage bitmapImage = new BitmapImage(new Uri(bitmapPath, UriKind.Relative));
            image1.Source = bitmapImage;

        }

        private void MainWindow_MouseEnter(object sender, MouseEventArgs e)
        {
            Background.Opacity = 1;
            this.Height = 33;

            string bitmapPath = @"Resources/Detalhamento1.png";
            BitmapImage bitmapImage = new BitmapImage(new Uri(bitmapPath, UriKind.Relative));
            image1.Source = bitmapImage;
        }

        private void MainWindow_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left)
                visivel = !visivel;
            //if (e.ChangedButton == MouseButton.Left)
            //    DragMove();
        }

        public string GerarSenhaDiaria()
        {
            double nCalculo;
            DateTime dData;
            dData = DateTime.Now;
            nCalculo = dData.Day * 45.81;
            nCalculo = Math.Pow((nCalculo / 7.25), dData.Day);
            nCalculo = (nCalculo * dData.Month) / 34.59;
            nCalculo = (((nCalculo / 56.18) + dData.Month) / 23.46);
            nCalculo = (nCalculo / dData.Year) * 9.74;
            string resultado = nCalculo.ToString().Replace(".", "").Replace(",", "");
            if (resultado.Length > 16)
                resultado = nCalculo.ToString("0.00000000000##e+00").Replace(".", "").Replace(",", "");
            resultado = resultado.Substring(2, 6);
            return resultado;
        }

        public string GeraSenhaRestrita()
        {
            double nCalculo;
            DateTime dData;
            dData = DateTime.Now;
            nCalculo = dData.Day * 47.17;
            nCalculo = Math.Pow((nCalculo / 9.21) ,dData.Day);
            nCalculo = (nCalculo * dData.Month) / 23.71;
            nCalculo = (((nCalculo / 56.18) + dData.Month) / 37.19);
            nCalculo = (nCalculo / dData.Year) * 3.27;
            string resultado = nCalculo.ToString().Replace(".", "").Replace(",", "");
            if (resultado.Length > 16)
                resultado = nCalculo.ToString("0.00000000000##e+00").Replace(".", "").Replace(",", "");
            resultado = resultado.Substring(2, 6);
            return resultado;
        }
    }
}
