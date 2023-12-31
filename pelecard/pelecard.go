package pelecard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"prioRecurr2Civi/types"
)

type PeleCard struct {
	Url string `json:"-"`

	User     string `json:"user"`
	Password string `json:"password"`
	Terminal string `json:"terminalNumber"`
}

func (p *PeleCard) Init(terminal, user, password string) (err error) {
	if user == "" || password == "" || terminal == "" {
		err = fmt.Errorf("PELECARD parameters are missing")
		return
	}
	p.User = user
	p.Password = password
	p.Terminal = terminal
	p.Url = "https://gateway20.pelecard.biz:443/services"

	return
}

func (p *PeleCard) GetTransData(start, end string) (err error, response []types.GetTransDataResponse) {
	var v = struct {
		PeleCard
		StartDate string `json:",omitempty"`
		EndDate   string `json:",omitempty"`
	}{
		PeleCard:  *p,
		StartDate: start,
		EndDate:   end,
	}
	params, _ := json.Marshal(v)
	resp, err := http.Post(p.Url+"/GetTransData", "application/json", bytes.NewBuffer(params))
	if err != nil {
		return
	}

	defer resp.Body.Close()
	var body map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if status, ok := body["StatusCode"]; ok {
		if status == "000" {
			data := body["ResultData"]
			items, _ := json.Marshal(data)
			_ = json.Unmarshal(items, &response)
		} else {
			err = fmt.Errorf("%s: %s", status, body["ErrorMessage"])
		}
	}
	return
}

func (p *PeleCard) connect(action string) (err error, result map[string]interface{}) {
	params, _ := json.Marshal(*p)
	resp, err := http.Post(p.Url+action, "application/json", bytes.NewBuffer(params))

	if err != nil {
		return
	}
	defer resp.Body.Close()
	var body map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if urlOk, ok := body["URL"]; ok {
		if urlOk.(string) != "" {
			result = make(map[string]interface{})
			result["URL"] = urlOk.(string)
			return
		}
	}
	if msg, ok := body["Error"]; ok {
		msg := msg.(map[string]interface{})
		if errCode, ok := msg["ErrCode"]; ok {
			if errCode.(float64) > 0 {
				err = fmt.Errorf("%d: %s", int(errCode.(float64)), msg["ErrMsg"])
			}
		} else {
			err = fmt.Errorf("0: %s", msg["ErrMsg"])
		}
	} else {
		if status, ok := body["StatusCode"]; ok {
			if status == "000" {
				err = nil
				result = body["ResultData"].(map[string]interface{})
			} else {
				err = fmt.Errorf("%s: %s", status, body["ErrorMessage"])
			}
		}
	}

	return
}
