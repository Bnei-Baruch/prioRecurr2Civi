// CGO_ENABLED=0 go build prioRecurr2Civi.go && strip prioRecurr2Civi  && /usr/bin/upx -9 prioRecurr2Civi && cp prioRecurr2Civi /media/sf_D_DRIVE/projects/bbpriority/prioRecurr2Civi
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/joho/godotenv/autoload"
	"github.com/pkg/errors"

	"prioRecurr2Civi/pelecard"
	"prioRecurr2Civi/types"
)

var prioApiUrl = os.Getenv("PRIO_API_URL")
var prioApiOrg = os.Getenv("PRIO_API_ORG")
var prioApiUser = os.Getenv("PRIO_API_USER")
var prioApiPassword = os.Getenv("PRIO_API_PASSWORD")

var civiApiKey = os.Getenv("CIVI_API_KEY")
var civiSiteKey = os.Getenv("CIVI_SITE_KEY")
var civiSiteUrl = os.Getenv("CIVI_SITE_URL")

var civiPaymentProcessor = "75"

var findContactPattern = fmt.Sprintf("%s?entity=Contribution&action=get&api_key=%s&key=%s&json={\"return\":\"payment_instrument_id,campaign_id,financial_type_id,contribution_id,contribution_page_id,contact_id,currency,total_amount,contribution_source,\",\"id\":%%s}", civiSiteUrl, civiApiKey, civiSiteKey)
var findFinancialTypePattern = fmt.Sprintf("%s?entity=FinancialType&action=get&api_key=%s&key=%s&json={\"id\":%%s}", civiSiteUrl, civiApiKey, civiSiteKey)
var createContributionUrl = fmt.Sprintf("%s?entity=Contribution&action=create&api_key=%s&key=%s&json=1", civiSiteUrl, civiApiKey, civiSiteKey)

const insertBbPaymentResponse = `
			INSERT INTO civicrm_bb_payment_responses(trxn_id, cid, cardtype, cardnum, cardexp, firstpay, installments, response, amount, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,  NOW())
		`
const checkBbPaymentTrxnId = `SELECT count(1) as found FROM civicrm_bb_payment_responses WHERE trxn_id = ?`

type ResponseStruct struct {
	Value []map[string]interface{} `json:"value"`
}

type Contribution struct {
	ID          string  `json:"contribution_id"`
	PayDate     string  `json:"pay_date"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Sku         string  `json:"sku"`
	Description string  `json:"description"`
	Pelecard    types.GetTransDataResponse
}

type CiviGetContribution struct {
	ID                  string `json:"contribution_id"`
	ContactId           string `json:"contact_id"`
	Currency            string `json:"currency"`
	Amount              string `json:"total_amount"`
	ContributionSource  string `json:"contribution_source"`
	FinancialTypeId     string `json:"financial_type_id"`
	ContributionPageId  string `json:"contribution_page_id"`
	PaymentInstrumentId string `json:"payment_instrument_id"`
	CampaignId          string `json:"campaign_id"`
}

type CiviGetContributions struct {
	ID      int `json:"id"`
	IsError int
	Values  map[string]CiviGetContribution `json:"values"`
}

type CiviGetFinancialTypeData struct {
	Name string
}

type CiviGetFinancialType struct {
	Values map[string]CiviGetFinancialTypeData `json:"values"`
}

type Terminal struct {
	Terminal string
	User     string
	Password string
	Name     string
}

var db *sqlx.DB

// Map new price to old ones
var priceChanges map[int][]int = map[int][]int{
	180: []int{100},
}

func main() {
	t := time.Now()
	y, m, _ := t.Date()
	if len(os.Args) == 3 {
		y, _ = strconv.Atoi(os.Args[1])
		x, _ := strconv.Atoi(os.Args[2])
		m = time.Month(x)
	}
	loc := t.Location()
	firstDay := time.Date(y, m, 1, 0, 0, 0, 0, loc)
	lastDay := time.Date(y, m+1, 1, 0, 0, 0, 0, loc)
	from := firstDay.Format("02/01/2006 15:04")
	to := lastDay.Format("02/01/2006 15:04")
	fmt.Println("First day: ", from, " Last day: ", to)

	db = OpenDb()
	defer db.Close()

	terminals := []*Terminal{
		{
			Name:     "BB Payments Terminal",
			Terminal: os.Getenv("ben2_PELECARD_TERMINAL"),
			User:     os.Getenv("ben2_PELECARD_USER"),
			Password: os.Getenv("ben2_PELECARD_PASSWORD"),
		},
		{
			Name:     "BB Recurrent Payments Terminal",
			Terminal: os.Getenv("PELECARD_RECURR_TERMINAL"),
			User:     os.Getenv("PELECARD_USER"),
			Password: os.Getenv("PELECARD_PASSWORD"),
		},
		{
			Name:     "Family Payments Terminal",
			Terminal: os.Getenv("meshp18_PELECARD_TERMINAL"),
			User:     os.Getenv("meshp18_PELECARD_USER"),
			Password: os.Getenv("meshp18_PELECARD_PASSWORD"),
		},
		{
			Name:     "Arvut Payments Terminal",
			Terminal: os.Getenv("PELECARD_TERMINAL1"),
			User:     os.Getenv("PELECARD_USER1"),
			Password: os.Getenv("PELECARD_PASSWORD1"),
		},
	}
	for _, terminal := range terminals {
		handleTerminal(terminal, from, to)
	}
	log.Println("Done")
}

func handleTerminal(terminal *Terminal, from, to string) {
	var err error

	fmt.Printf("\n\nTerminal: %s\n\n", terminal.Name)

	// 1. Get list of payments from Pelecard for yesterday and filter out those starting with civicrm
	payments, err := GetListFromPelecard(terminal, from, to)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("Got ", len(payments), " payments")
	if len(payments) == 0 {
		return
	}

	// 2. Get from Priority list of contribution ids
	contributions, err := GetPriorityContributions(payments)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("Got ", len(contributions), " contributions")

	// Get from Civi customer, amount and currency of the above contribution
	// Create new contribution marked as Done
	err = HandleContributions(contributions)
	if err != nil {
		fmt.Println(err)
		return
	}
}

var paymentFromPrio = regexp.MustCompile(`^\d+$`)

func GetListFromPelecard(terminal *Terminal, from, to string) (payments []types.GetTransDataResponse, err error) {
	fmt.Println("--> GetListFromPelecard")
	card := &pelecard.PeleCard{}
	if err = card.Init(terminal.Terminal, terminal.User, terminal.Password); err != nil {
		return nil, errors.Wrapf(err, "GetListFromPelecard:: Unable to initialize: %s", err.Error())
	}
	err, response := card.GetTransData(from, to)
	if err != nil {
		return nil, errors.Wrapf(err, "GetTransData: error")
	}
	for _, item := range response {
		if paymentFromPrio.MatchString(item.ParamX) {
			var count int
			if err = db.Get(&count, checkBbPaymentTrxnId, item.TrxnId); err != nil {
				fmt.Println(item.ParamX, " -- Error -- ", err)
				continue
			}
			if count > 0 {
				fmt.Printf("paramX: %s, count: %d -- SKIP -- record already exists\n", item.ParamX, count)
				continue
			}

			payments = append(payments, item)
		}
	}
	return
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func OpenDb() (db *sqlx.DB) {
	var err error

	host := os.Getenv("CIVI_HOST")
	if host == "" {
		log.Fatalf("Unable to connect without host\n")
	}
	dbName := os.Getenv("CIVI_DBNAME")
	if dbName == "" {
		log.Fatalf("Unable to connect without \tdbName := os.Getenv(\"CIVI_DBNAME\")\n\n")
	}
	user := os.Getenv("CIVI_USER")
	if user == "" {
		log.Fatalf("Unable to connect without username\n")
	}
	password := os.Getenv("CIVI_PASSWORD")
	if password == "" {
		log.Fatalf("Unable to connect without password\n")
	}
	protocol := os.Getenv("CIVI_PROTOCOL")
	if protocol == "" {
		log.Fatalf("Unable to connect without protocol\n")
	}

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s", user, password, protocol, host, dbName)
	if db, err = sqlx.Open("mysql", dsn); err != nil {
		log.Fatalf("DB connection error: %v\n", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
	}

	return
}

func HandleContributions(contributions []Contribution) (err error) {
	fmt.Println("---> HandleContributions: ", len(contributions))
	for _, contribution := range contributions {
		if contribution.ID == "40" || contribution.ID == "57" || contribution.ID == "51" || contribution.ID == "130" || contribution.ID == "151" {
			continue
		}

		//fmt.Print("paramX: ", contribution.ID)

		uri := fmt.Sprintf(findContactPattern, contribution.ID)
		resp, err := http.Get(uri)
		if err != nil {
			fmt.Printf(" -- Unable to find Contact %s: %#v\n%s\n", contribution.ID, err, uri)
			continue
		}
		payment := CiviGetContributions{}
		_ = json.NewDecoder(resp.Body).Decode(&payment)
		paymentValues := payment.Values[contribution.ID]

		// Find financialTypeId
		uri = fmt.Sprintf(findFinancialTypePattern, paymentValues.FinancialTypeId)
		resp, err = http.Get(uri)
		if err != nil {
			fmt.Printf(" -- Get financial Type %s error: %v\n", paymentValues.FinancialTypeId, err)
			continue
		}
		financialType := CiviGetFinancialType{}
		_ = json.NewDecoder(resp.Body).Decode(&financialType)

		financialTypeId := financialType.Values[paymentValues.FinancialTypeId].Name

		// Create new contribution marked as Done
		var formData = url.Values{
			"total_amount":          {strconv.Itoa(int(contribution.Amount))},
			"currency":              {contribution.Currency},
			"financial_type_id":     {financialTypeId},
			"receive_date":          {contribution.PayDate},
			"contact_id":            {paymentValues.ContactId},
			"contribution_page_id":  {paymentValues.ContributionPageId},
			"source":                {contribution.Description + " (recurrent)"},
			"payment_instrument_id": {paymentValues.PaymentInstrumentId},
			"campaign_id":           {paymentValues.CampaignId},
			"tax_amount":            {"0"},
			"invoice_number":        {"1"},                  // do not send to Priority
			"payment_processor":     {civiPaymentProcessor}, //
			"custom_941":            {"2"},                  // Monthly donation
			"custom_942":            {"1"},                  // Credit Card
		}
		fmt.Printf(" --->>> Create new contribution\n\t%#v\n", formData)
		resp, err = http.PostForm(createContributionUrl, formData)
		if err != nil {
			fmt.Printf(" -- Unable create new contribution: %#v\n", err)
			continue
		}
		response := map[string]interface{}{}
		_ = json.NewDecoder(resp.Body).Decode(&response)
		if response["is_error"].(float64) == 1 {
			fmt.Printf(" -- Create new contribution (%#v) error: %s\n", response, response["error_message"].(string))
			continue
		}

		id := int(response["id"].(float64))
		amount, _ := strconv.ParseFloat(contribution.Pelecard.Amount, 64)
		amount /= 100
		p, err := json.Marshal(contribution.Pelecard)
		if err != nil {
			log.Fatalf("Marshal error: %v\n", err)
		}
		_, err = db.Exec(insertBbPaymentResponse,
			contribution.Pelecard.TrxnId,
			id,
			contribution.Pelecard.CardType,
			contribution.Pelecard.CardNum,
			contribution.Pelecard.CardExp,
			amount,
			contribution.Pelecard.Installments,
			p,
			amount,
		)
		if err != nil {
			log.Fatalf("DB INSERT error: %v\n", err)
		}
		fmt.Printf(" -- Created record id %d\n", id)
	}

	return
}

func GetPriorityContributions(payments []types.GetTransDataResponse) (contributions []Contribution, err error) {
	urlBase := prioApiUrl + prioApiOrg
	fmt.Println("--> GetPriorityContributions")

	for _, payment := range payments {
		fmt.Printf("--> Payment %s: %#v\n", payment.ParamX, payment)
		uri := urlBase + "/PAYMENT2_CHANGES?$filter=PAYMENT eq " + payment.ParamX + "&$select=IVNUM"
		data, err := getPelecardData(uri)
		fmt.Printf("PAYMENT2_CHANGES for %s: %#v\n", payment.ParamX, data)
		if err != nil {
			return nil, errors.Wrapf(err, "PAYMENT2_CHANGES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) != 1 {
			log.Printf("############## Payment %s: [1] Data is not an array: %#v\n", payment.ParamX, data.Value)
			continue
		}
		ivnum := data.Value[0]["IVNUM"].(string)
		uri = urlBase + "/TINVOICES?$filter=IVNUM eq '" + ivnum + "'&$expand=TPAYMENT2_SUBFORM($select=CCUID,PAYDATE),TFNCITEMS_SUBFORM($select=FNCIREF1)"
		data, err = getPelecardData(uri)
		fmt.Printf("TINVOICES for %s, %d: %#v\n", ivnum, len(data.Value), data)
		if err != nil {
			return nil, errors.Wrapf(err, "TINVOICES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) == 0 {
			//log.Printf("No priority Data: %s\n", uri)
			continue
		}
		value := data.Value[0]
		is46 := false
		if value["QAMT_PRINT46"] != nil {
			is46 = value["QAMT_PRINT46"].(string) == "D"
		}
		//if is46 != "D" { // Donation
		//	log.Printf("############## Payment is not DONATION, but >%s<\n", is46)
		//	continue
		//}
		if value["CUSTNAME"] == nil {
			fmt.Println("No CUSTNAME, continue")
			continue
		}
		custname := value["CUSTNAME"].(string)
		ivSubnum := getByRef[string](data.Value[0], "TFNCITEMS_SUBFORM", "FNCIREF1")[0]
		results := getByRef[string](data.Value[0], "TPAYMENT2_SUBFORM", "CCUID", "PAYDATE")
		//token := results[0][5:15]
		payDate := results[1]
		uri = urlBase + "/CINVOICES?$filter=IVNUM eq '" + ivSubnum + "'&$expand=CINVOICEITEMS_SUBFORM($select=PRICE,ICODE,ACCNAME,DSI_DETAILS)"
		data, err = getPelecardData(uri)
		fmt.Printf("CINVOICES for %s: %#v\n", ivSubnum, data)
		if err != nil {
			return nil, errors.Wrapf(err, "CINVOICES for %s: error\n", payment.ParamX)
		}
		if len(data.Value) != 1 {
			log.Printf("############## Payment %s: [2] Data is not array: %#v\n", payment.ParamX, data.Value)
			continue
		}
		results = getByRef[string](data.Value[0], "CINVOICEITEMS_SUBFORM", "ICODE", "ACCNAME", "DSI_DETAILS")
		currency := results[0]
		sku := results[1]
		description := results[2]
		amount := getByRef[int](data.Value[0], "CINVOICEITEMS_SUBFORM", "PRICE")[0]
		uri = urlBase + "/QAMO_LOADINTENET?$filter=QAMO_CUSTNAME eq '" + custname + "'&$select=QAMT_REFRENCE,QAMO_PRICE,QAMO_CURRNCY,QAMO_PARTNAME,QAMO_MONTHLY"
		data, err = getPelecardData(uri)
		fmt.Printf("QAMO_LOADINTENET for %s: %#v\n", custname, data)
		if err != nil {
			return nil, errors.Wrapf(err, "QAMO_LOADINTENET for %s: error\n", payment.ParamX)
		}
		fmt.Printf("search for amount %d, currency %s, sku %s\n", amount, currency, sku)
		contributionId := searchAmountCurrencySKU(data.Value, is46, amount, currency, sku)
		if contributionId == "" {
			fmt.Printf("exact amount not found\n")
			// Search for a known price changes replacements are here
			possibleAmounts := priceChanges[amount]
			for _, pAmount := range possibleAmounts {
				contributionId = searchAmountCurrencySKU(data.Value, is46, pAmount, currency, sku)
				if contributionId != "" {
					fmt.Printf("  ===> FOUND possible (%d) contributionId: %s\n", pAmount, contributionId)
					break
				}
			}
		}
		if contributionId == "" {
			//fmt.Printf("contributionId not found\n")
			continue
		}
		fmt.Printf("  ===> FOUND contributionId: %s\n", contributionId)
		if currency == "ש\"ח" {
			currency = "ILS"
		}
		contributions = append(contributions, Contribution{
			ID:          contributionId,
			PayDate:     payDate,
			Amount:      float64(amount),
			Currency:    currency,
			Sku:         sku,
			Description: description,
			Pelecard:    payment,
		})
	}
	//fmt.Print("\n")
	return
}

func searchAmountCurrencySKU(values []map[string]interface{}, is46 bool, amount int, currency string, sku string) string {
	for _, value := range values {
		monthly := false
		if value["QAMO_MONTHLY"] != nil {
			monthly = value["QAMO_MONTHLY"].(string) == "Y"
		}
		if !(is46 || monthly) {
			continue
		}
		if value["QAMO_PRICE"].(float64) == float64(amount) &&
			value["QAMO_CURRNCY"].(string) == currency &&
			value["QAMO_PARTNAME"].(string) == sku {
			return value["QAMT_REFRENCE"].(string)
		}
	}
	return ""
}

func getPelecardData(uri string) (data ResponseStruct, err error) {
	uri = strings.Replace(uri, " ", "%20", -1)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return data, errors.Wrapf(err, "getPelecardData: Unable to initialize: %s", uri)
	}
	req.Header.Set("Authorization", "Basic "+basicAuth(prioApiUser, prioApiPassword))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return data, errors.Wrapf(err, "getPelecardData: Unable to Get response: %s", uri)
	}
	if resp.StatusCode == 200 { // OK
		_ = json.NewDecoder(resp.Body).Decode(&data)
	} else {
		return data, errors.Wrapf(err, "getPelecardData Server Error: %#v", resp)
	}
	return
}

func getByRef[T any](ref map[string]interface{}, idx1 string, idx2 ...string) (res []T) {
	body, _ := json.Marshal(ref[idx1])
	var x []map[string]T
	_ = json.Unmarshal(body, &x)
	for _, idx := range idx2 {
		res = append(res, x[0][idx])
	}
	return
}
