import wx
import datetime


class Mywin(wx.Frame):

    def __init__(self, parent, title):
        width, height = wx.GetDisplaySize()
        super(Mywin, self).__init__(parent, title=title, size=wx.Size(350, 250))
        self.InitUI()

    def InitUI(self):
        pnl = wx.Panel(self)
        vbox = wx.BoxSizer(wx.VERTICAL)

        self.start_dt = datetime.date(2019, 7, 1)
        self.dt_label = wx.StaticText(pnl, label='date: {0}'.format(self.start_dt))
        vbox.Add(self.dt_label, 1, wx.ALIGN_CENTRE_HORIZONTAL)

        self.slider = wx.Slider(pnl, value=0, minValue=0, maxValue=30, style=wx.SL_HORIZONTAL)
        self.slider.Bind(wx.EVT_SLIDER, self.OnSliderScroll)
        vbox.Add(self.slider, 3, flag=wx.EXPAND | wx.ALIGN_CENTER_HORIZONTAL | wx.TOP, border=20)

        self.subs_only = wx.StaticText(pnl, label='Subs_only: = {0}'.format(self.slider.GetMax()), style=wx.ALIGN_LEFT)
        vbox.Add(self.subs_only, 1, wx.ALIGN_CENTRE_HORIZONTAL)

        self.pkg_claim = wx.StaticText(pnl, label='pkg_claim: = {0}'.format(self.slider.GetMin()), style=wx.ALIGN_RIGHT)
        vbox.Add(self.pkg_claim, 1, wx.ALIGN_CENTRE_HORIZONTAL)

        pnl.SetSizer(vbox)
        self.Centre()
        self.Show(True)

    def OnSliderScroll(self, e):
        obj = e.GetEventObject()
        val = obj.GetValue()
        t = datetime.timedelta(days=val)
        self.dt_label.SetLabel('date: {0}'.format(self.start_dt + t))
        self.subs_only.SetLabel('Subs_only: = {0}'.format(0 if self.slider.GetMax() - val * 3 <= 0 else self.slider.GetMax() - val * 3))
        self.pkg_claim.SetLabel('pkg_claim: = {0}'.format(self.slider.GetMin() + val * 3))

def main():
    ex=wx.App()
    Mywin(None, 'BC1 demo 2020')
    ex.MainLoop()


if __name__ == '__main__':
    main()
