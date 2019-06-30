import wx
import wx.dataview
import random
import datetime

RAND_MAX = 99999999999999999999

items = 0


def main():
    def lol(event):
        r = data_view.GetScrollRange(wx.VERTICAL)
        s = data_view.GetScrollThumb(wx.VERTICAL)
        pos = data_view.GetScrollPos(wx.VERTICAL)
        # print('r({0}) - s({1}) == pos({2}) == {3}'.format(r,s,pos,r - s == pos))
        if r - s == pos:
            gen_data()
        event.Skip()

    def gen_data():
        global items
        print(items)
        x = 0
        for x in range(1, 51):
            t = (items + x, random.randint(1, RAND_MAX), str(random.randint(1, RAND_MAX)), str(datetime.datetime.now()))
            data_view.AppendItem(tuple(map(str, t)))
        items += x

    app = wx.App(False)  # Create a new app, don't redirect stdout/stderr to a window.
    frame = wx.Frame(None, wx.ID_ANY, "Hello World", size=wx.Size(1000, 500))  # A Frame is a top-level window.

    headers = ('id', 'rand_int', 'rand_str', 'datetime')

    data_view = wx.dataview.DataViewListCtrl(frame, wx.ID_ANY)

    for h in headers:
        data_view.AppendTextColumn(h, width=wx.COL_WIDTH_AUTOSIZE, mode=wx.dataview.DATAVIEW_CELL_ACTIVATABLE,
                                   flags=wx.dataview.DATAVIEW_COL_SORTABLE | wx.dataview.DATAVIEW_COL_RESIZABLE)

    data_view.Bind(wx.EVT_SCROLLWIN, lol)

    print(data_view.GetSortingColumn())
    # data_view.Bind(wx.EVT_DATAVIEW_COLUMN_HEADER_CLICK, sort)
    gen_data()
    frame.Show(True)  # Show the frame.
    app.MainLoop()


main()
