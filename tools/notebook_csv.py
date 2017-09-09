from csv import DictReader
from IPython.display import HTML

def _gen_header(header):
    return '<th valign="top" nowrap="" style="border: 1px solid #CCC; text-align:center;">{}</th>'.format(header)

def _gen_headers(headers):
    return '<thead>{}</thead>'.format(
        ''.join(map(_gen_header, headers))
    )

def _gen_cell(text):
    return '<td valign="top" style="border: 1px solid #CCC; text-align:center;" nowrap="">{}</td>'.format(text)

def _gen_row(row, headers):
    return '<tr>{}</tr>'.format(
        ''.join([ _gen_cell(row[header]) for header in headers ])
    )

def _gen_rows(rows, headers):
    return '<tbody>{}</tdboy'.format(
        ''.join([ _gen_row(row, headers) for row in rows ])
    )

def _gen_html(headers, rows):
    return """
<table
    border="0"
    cellpadding="2"
    cellspacing="0"
    style="font-size:12px;
    font-face:Helvetica Neue, Arial;
    border: 1px solid #CCC;"
    width="900"
>
  {}
  {}
  <tr><td style="text-align: center" colspan={}>...</td></tr>
  {}
</table>
    """.format(
        _gen_headers(headers),
        _gen_rows(rows[:10], headers),
        len(headers),
        _gen_rows(rows[-10:], headers),
    )

class CsvForNoteBook:
    def __init__(self, file_path, max_row=None):
        with open(file_path) as file:
            reader = DictReader(file, delimiter=';')
            if max_row:
                docs = [ next(reader) for i in range(max_row)]
            else:
                docs = list(reader)

        self._data = docs
        self._html = HTML(_gen_html(reader.fieldnames, docs))

    def show(self):
        return self._html

    @property
    def data(self):
        return self._data

def load(file_path, max_row=None):
    return CsvForNoteBook(file_path, max_row=max_row)
