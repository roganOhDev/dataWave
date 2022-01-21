import tkinter

window = tkinter.Tk()
window.title("testing")
window.geometry("640x400+100+100")
window.resizable(True,True)
listbox = tkinter.Listbox(window, selectmode='extended', height=0)
listbox.insert(0, "1번")
listbox.insert(1, "2번")
listbox.insert(2, "2번")
listbox.insert(3, "2번")
listbox.insert(4, "3번")
listbox.delete(1, 2)
listbox.pack()

window.mainloop()
#TODO: 프론트를 손으로 그려놔야 한다.