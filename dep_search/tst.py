
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch


import tkinter as tk


class PointsList:

    def __init__(self, line, arrows=[]):
        self.line = line
        self.points = {i: v for i, v in enumerate(zip(line.get_xdata(), line.get_ydata()))}
        self.moveble_line_idx = {}
        if arrows:
            self.arrows = arrows
        else:
            self.arrows = []

    def add_arrows(self, ar):
        self.arrows.append(ar)

    def connect_arrows_to_points(self):

        for i, l in enumerate(self.lines):

            psab = l._posA_posB


        #     if psab[0] == xy:
        #         moveble_line_idx[i] = 0
        #     if psab[1] == xy:
        #         moveble_line_idx[i] = 1

        # return moveble_line_idx


class DragableLineAnn:
    lock = None

    def __init__(self, myline, an):
        self.myline = myline
        self.line = myline.points
        self.an = an
        self.press = None
        self.background = None
        # self.background_an = None
        self.xs = list(myline.points.get_xdata())
        self.ys = list(myline.points.get_ydata())
        self.moving_point_idx = -1
        self.cidpress = myline.points.figure.canvas.mpl_connect('button_press_event', self.on_press)
        self.cidmotion = myline.points.figure.canvas.mpl_connect('motion_notify_event', self.on_motion)
        self.cidrelease = myline.points.figure.canvas.mpl_connect('button_release_event', self.on_release)
        # self.cidpick = myline.points.figure.canvas.mpl_connect('pick_event', self.on_pick)

    def on_pick(self, obj_name):

        root = tk.Tk()
        root.title('Table info for {0}'.format(obj_name))
        txt = tk.Text(root)
        txt.pack()
        txt.insert(tk.END, "TBD\ntable info for {0}".format(obj_name))
        txt.config(state=tk.DISABLED)
        tk.mainloop()

    def on_press(self, event):
        'on button press we will see if the mouse is over us and store some data'

        if event.inaxes != self.line.axes:
            return
        if DragableLineAnn.lock is not None:
            return
        contains, attrd = self.line.contains(event)
        if not contains:
            return

        DragableLineAnn.lock = self

        x0 = min(self.xs, key=lambda x: abs(x - event.xdata))
        y0 = min(self.ys, key=lambda x: abs(x - event.ydata))

        try:
            self.moving_point_idx = list(zip(self.xs, self.ys)).index((x0, y0))
        except Exception as e:
            print(e)
            self.moving_point_idx = -1
            self.on_release(event)
            return

        if event.dblclick:
            self.on_pick(self.an[self.moving_point_idx].get_text())
            return

        self.dep = self.myline.get_dep_line((x0, y0))
        self.dep_lines = [self.myline.lines[k] for k, _ in self.dep.items()]
        self.press = x0, y0, event.xdata, event.ydata
        print(self.press)

        canvas = self.line.figure.canvas
        canvases = []
        for dep_line in self.dep_lines:
            canvases.append(dep_line.figure.canvas)

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)

        axes_an = self.an[self.moving_point_idx].axes

        self.line.set_animated(True)
        for dep_line in self.dep_lines:
            dep_line.set_animated(True)

        self.an[self.moving_point_idx].set_animated(True)
        canvas.draw()

        self.background = canvas.copy_from_bbox(self.line.axes.bbox)
        axes.draw_artist(self.line)
        for i, dep_line in enumerate(self.dep_lines):
            axeses[i].draw_artist(dep_line)
        axes_an.draw_artist(self.an[self.moving_point_idx])

        # and blit just the redrawn area
        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes_an.bbox)

    def on_motion(self, event):
        'on motion we will move the line if the mouse is over us'
        if DragableLineAnn.lock is not self:
            return
        if event.inaxes != self.line.axes:
            return
        x0, y0, xpress, ypress = self.press
        dx = event.xdata - xpress
        dy = event.ydata - ypress
        self.xs[self.moving_point_idx] = (x0 + dx)
        self.ys[self.moving_point_idx] = (y0 + dy)
        self.line.set_xdata(self.xs)
        self.line.set_ydata(self.ys)
        self.an[self.moving_point_idx].xy = ((x0 + dx), (y0 + dy))
        # print(self.dep)
        for k, v in self.dep.items():
            dline = self.myline.lines[k]
            psab = dline._posA_posB
            psab[v] = ((x0 + dx), (y0 + dy))
            dline.set_positions(*psab)
            # dline.set_ydata(ydat)
            # print(list(zip(xdat, ydat)))

        canvas = self.line.figure.canvas
        canvases = []
        # for dep_line in self.dep_lines:
        #     canvases.append(dep_line.figure.canvas)

        axes = self.line.axes
        axeses = []
        for dep_line in self.dep_lines:
            axeses.append(dep_line.axes)
        axes2 = self.an[self.moving_point_idx].axes
        # restore the background region
        canvas.restore_region(self.background)
        # for i, c in enumerate(canvases):
        #     c.restore_region(self.backgroundes[i])

        # canvas.restore_region(self.background_an)

        # redraw just the current rectangle
        axes.draw_artist(self.line)
        for i, dl in enumerate(self.dep_lines):
            axeses[i].draw_artist(dl)
        axes2.draw_artist(self.an[self.moving_point_idx])

        # blit just the redrawn area
        canvas.blit(axes.bbox)
        for i, c in enumerate(canvases):
            c.blit(axeses[i].bbox)
        canvas.blit(axes2.bbox)

    def on_release(self, event):
        'on release we reset the press data'
        if DragableLineAnn.lock is not self:
            return

        self.press = None

        # turn off the rect animation property and reset the background
        self.line.set_animated(False)
        for dl in self.dep_lines:
            dl.set_animated(False)
        self.an[self.moving_point_idx].set_animated(False)
        self.background = None

        self.line.figure.canvas.draw()
        for dl in self.dep_lines:
            dl.figure.canvas.draw()
        self.an[self.moving_point_idx].figure.canvas.draw()

        DragableLineAnn.lock = None


fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_title('Data flow')


x = [1, 1, 2]
y = [1, 2, 2]


lines, = ax.plot(x, y, 'o', ms=10)
ml = MyLines(lines)
# ax.axes.get_xaxis().set_ticks([])
# ax.axes.get_yaxis().set_ticks([])


# l1 = mlines.Line2D([1, 1], [1, 2])
# ax.add_line(l1)
# l2 = mlines.Line2D([1, 2], [2, 2])
# ax.add_line(l2)

# ml.add_lines(l1)
# ml.add_lines(l2)


arrow1 = FancyArrowPatch((1, 1), (1, 2),
                         arrowstyle='-|>',
                         shrinkA=1,
                         shrinkB=1,
                         mutation_scale=10,
                         color='red',
                         linewidth=1.5,
                         connectionstyle='arc3,rad=0.2',
                         zorder=1)  # arrows go behind nodes

ax.add_patch(arrow1)
ml.add_lines(arrow1)
arrow2 = FancyArrowPatch((1, 2), (2, 2),
                         arrowstyle='-|>',
                         shrinkA=1,
                         shrinkB=1,
                         mutation_scale=10,
                         color='red',
                         linewidth=1.5,
                         connectionstyle='arc3,rad=0.2',
                         zorder=1)  # arrows go behind nodes

ax.add_patch(arrow2)
ml.add_lines(arrow2)

an = []
dlol = []
for t in zip(x, y):
    an.append(ax.annotate(str(t), t, textcoords="offset points", xytext=(0, 11), ha='center', color='red'))

dl = DragableLineAnn(ml, an)


plt.show()

'''
import numpy as np
import matplotlib.pyplot as plt

X = np.random.rand(100, 1000)
xs = np.mean(X, axis=1)
ys = np.std(X, axis=1)

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_title('click on point to plot time series')
line, = ax.plot(xs, ys, 'o', picker=5)  # 5 points tolerance


def onpick(event):

    if event.artist!=line: return True

    N = len(event.ind)
    if not N: return True


    figi = plt.figure()
    for subplotnum, dataind in enumerate(event.ind):
        ax = figi.add_subplot(N,1,subplotnum+1)
        ax.plot(X[dataind])
        ax.text(0.05, 0.9, 'mu=%1.3f\nsigma=%1.3f'%(xs[dataind], ys[dataind]),
                transform=ax.transAxes, va='top')
        ax.set_ylim(-0.5, 1.5)
    figi.show()
    return True

fig.canvas.mpl_connect('pick_event', onpick)

plt.show()'''
