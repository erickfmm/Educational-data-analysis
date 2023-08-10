import os
import sys
import fitz

def read_text(folder):
    newfolder = "txt_"+folder
    if not os.path.exists(newfolder):
        os.mkdir(newfolder)
    for filename in os.listdir(folder):
        filename : str = filename
        if not filename.endswith(".pdf"):
            continue
        print(filename)
        doc = fitz.open(os.path.join(folder, filename))
        text = ""
        for page in doc:
            text+=page.get_text()
        with open(os.path.join(newfolder, f"text_{filename}.txt"), "w", encoding="utf-8", errors="warn") as fh:
            fh.write(text)
        
if __name__ == "__main__":
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    read_text("files_pei")