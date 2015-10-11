package web

import (
	"bytes"
	"html/template"
	"path/filepath"
)

// consume list of templates and release their full path counterparts
func fileNames(tdir string, filenames ...string) []string {
	flist := []string{}
	for _, fname := range filenames {
		flist = append(flist, filepath.Join(tdir, fname))
	}
	return flist
}

// parse template with given data
func parseTmpl(tdir, tmpl string, data interface{}) string {
	buf := new(bytes.Buffer)
	filenames := fileNames(tdir, tmpl)
	t := template.Must(template.ParseFiles(filenames...))
	err := t.Execute(buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

// Templates interface
type DASTemplates struct {
	top, bottom, searchForm, cards, dasError string
}

func (q DASTemplates) Top(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(_tdir, "top.tmpl", tmplData)
	return q.top
}

func (q DASTemplates) Bottom(tdir string, tmplData map[string]interface{}) string {
	if q.bottom != "" {
		return q.bottom
	}
	q.bottom = parseTmpl(_tdir, "bottom.tmpl", tmplData)
	return q.bottom
}

func (q DASTemplates) SearchForm(tdir string, tmplData map[string]interface{}) string {
	if q.searchForm != "" {
		return q.searchForm
	}
	q.searchForm = parseTmpl(_tdir, "searchform.tmpl", tmplData)
	return q.searchForm
}

func (q DASTemplates) Cards(tdir string, tmplData map[string]interface{}) string {
	tmplData["CardsClass"] = "hide"
	if q.cards != "" {
		return q.cards
	}
	q.cards = parseTmpl(_tdir, "cards.tmpl", tmplData)
	return q.cards
}

func (q DASTemplates) Pagination(tdir string, tmplData map[string]interface{}) string {
	if q.searchForm != "" {
		return q.searchForm
	}
	q.searchForm = parseTmpl(_tdir, "pagination.tmpl", tmplData)
	return q.searchForm
}

func (q DASTemplates) DASError(tdir string, tmplData map[string]interface{}) string {
	if q.dasError != "" {
		return q.dasError
	}
	q.dasError = parseTmpl(_tdir, "error.tmpl", tmplData)
	return q.dasError
}
