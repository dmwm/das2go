package web

import (
	"bytes"
	"html/template"
	"path/filepath"

	"github.com/dmwm/das2go/config"
)

// global map of templates
var DASTemplateMap map[string]*template.Template

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
	if DASTemplateMap == nil {
		DASTemplateMap = make(map[string]*template.Template)
	}
	buf := new(bytes.Buffer)
	var err error
	if t, ok := DASTemplateMap[tmpl]; ok {
		err = t.Execute(buf, data)
	} else {
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
		DASTemplateMap[tmpl] = t
		err = t.Execute(buf, data)
	}
	if err != nil {
		panic(err)
	}
	return buf.String()
}

// DASTemplates structure
type DASTemplates struct {
	top, bottom, searchForm, cards, dasError, dasKeys, dasZero string
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

// DASZeroResults method for DASTemplates structure
func (q DASTemplates) DASZeroResults(tdir string, tmplData map[string]interface{}) string {
	if q.dasZero != "" {
		return q.dasZero
	}
	q.dasZero = parseTmpl(config.Config.Templates, "zero_results.tmpl", tmplData)
	return q.dasZero
}

// Status method for DASTemplates structure
func (q DASTemplates) Status(tdir string, tmplData map[string]interface{}) string {
	if q.dasError != "" {
		return q.dasError
	}
	q.dasError = parseTmpl(config.Config.Templates, "status.tmpl", tmplData)
	return q.dasError
}

// DasKeys method for DASTemplates structure
func (q DASTemplates) DasKeys(tdir string, tmplData map[string]interface{}) string {
	if q.dasKeys != "" {
		return q.dasKeys
	}
	q.dasKeys = parseTmpl(config.Config.Templates, "das_keys.tmpl", tmplData)
	return q.dasKeys
}
