package web

import (
	"bytes"
	"html/template"
	"path/filepath"

	"github.com/dmwm/das2go/config"
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
	funcMap := template.FuncMap{
		// The name "oddFunc" is what the function will be called in the template text.
		"oddFunc": func(i int) bool {
			if i%2 == 0 {
				return true
			}
			return false
		},
	}
	t := template.Must(template.New(tmpl).Funcs(funcMap).ParseFiles(filenames...))
	err := t.Execute(buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

// DASTemplates structure
type DASTemplates struct {
	top, bottom, searchForm, cards, dasError string
}

// Top method for DASTemplates structure
func (q DASTemplates) Top(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "top.tmpl", tmplData)
	return q.top
}

// Bottom method for DASTemplates structure
func (q DASTemplates) Bottom(tdir string, tmplData map[string]interface{}) string {
	if q.bottom != "" {
		return q.bottom
	}
	q.bottom = parseTmpl(config.Config.Templates, "bottom.tmpl", tmplData)
	return q.bottom
}

// SearchForm method for DASTemplates structure
func (q DASTemplates) SearchForm(tdir string, tmplData map[string]interface{}) string {
	if q.searchForm != "" {
		return q.searchForm
	}
	q.searchForm = parseTmpl(config.Config.Templates, "searchform.tmpl", tmplData)
	return q.searchForm
}

// Cards method for DASTemplates structure
func (q DASTemplates) Cards(tdir string, tmplData map[string]interface{}) string {
	tmplData["CardsClass"] = "hide"
	if q.cards != "" {
		return q.cards
	}
	q.cards = parseTmpl(config.Config.Templates, "cards.tmpl", tmplData)
	return q.cards
}

// FAQ method for DASTemplates structure
func (q DASTemplates) FAQ(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "faq.tmpl", tmplData)
	return q.top
}

// Guide method for DASTemplates structure
func (q DASTemplates) Guide(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "dbsql_vs_dasql.tmpl", tmplData)
	return q.top
}

// ApiRecord method for DASTemplates structure
func (q DASTemplates) ApiRecord(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "api_record.tmpl", tmplData)
	return q.top
}

// Keys method for DASTemplates structure
func (q DASTemplates) Keys(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "keys.tmpl", tmplData)
	return q.top
}

// Services method for DASTemplates structure
func (q DASTemplates) Services(tdir string, tmplData map[string]interface{}) string {
	if q.top != "" {
		return q.top
	}
	q.top = parseTmpl(config.Config.Templates, "services.tmpl", tmplData)
	return q.top
}

// Pagination  method for DASTemplates structure
func (q DASTemplates) Pagination(tdir string, tmplData map[string]interface{}) string {
	if q.searchForm != "" {
		return q.searchForm
	}
	q.searchForm = parseTmpl(config.Config.Templates, "pagination.tmpl", tmplData)
	return q.searchForm
}

// DASRequest method for DASTemplates structure
func (q DASTemplates) DASRequest(tdir string, tmplData map[string]interface{}) string {
	if q.dasError != "" {
		return q.dasError
	}
	q.dasError = parseTmpl(config.Config.Templates, "request.tmpl", tmplData)
	return q.dasError
}

// DASError method for DASTemplates structure
func (q DASTemplates) DASError(tdir string, tmplData map[string]interface{}) string {
	if q.dasError != "" {
		return q.dasError
	}
	q.dasError = parseTmpl(config.Config.Templates, "error.tmpl", tmplData)
	return q.dasError
}

// Status method for DASTemplates structure
func (q DASTemplates) Status(tdir string, tmplData map[string]interface{}) string {
	if q.dasError != "" {
		return q.dasError
	}
	q.dasError = parseTmpl(config.Config.Templates, "status.tmpl", tmplData)
	return q.dasError
}
