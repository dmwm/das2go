package utils

// DASServerError and others are represent different types of errors in DAS
const (
	_ = iota
	DASServerError
	DBSError
	PhedexError
	RucioError
	DynamoError
	ReqMgrError
	RunRegistryError
	McMError
	DashboardError
	SiteDBError
	CRICError
	CondDBError
	CombinedError
	MongoDBError
	DASProxyError
	DASQueryError
)

// DASServerErrorName and others provides human based definition of the DAS error
const (
	DASServerErrorName   = "DAS error"
	DBSErrorName         = "DBS upstream error"
	PhedexErrorName      = "PhEDEx upstream error"
	RucioErrorName       = "Rucio upstream error"
	DynamoErrorName      = "Dynamo upstream error"
	ReqMgrErrorName      = "ReqMgr upstream error"
	RunRegistryErrorName = "RunRegistry upstream error"
	McMErrorName         = "McM upstream error"
	DashboardErrorName   = "Dashboard upstream error"
	SiteDBErrorName      = "SiteDB upstream error"
	CRICErrorName        = "CRIC upstream error"
	CondDBErrorName      = "CondDB upstream error"
	CombinedErrorName    = "Combined error"
	MongoDBErrorName     = "MongoDB error"
	DASProxyErrorName    = "DAS proxy error"
	DASQueryErrorName    = "DAS query error"
)
