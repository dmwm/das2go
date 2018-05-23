package utils

const (
	_ = iota
	DASServerError
	DBSError
	PhedexError
	ReqMgrError
	RunRegistryError
	McMError
	DashboardError
	SiteDBError
	CondDBError
	CombinedError
	MongoDBError
	DASProxyError
	DASQueryError
)

const (
	DASServerErrorName   = "DAS error"
	DBSErrorName         = "DBS upstream error"
	PhedexErrorName      = "PhEDEx upstream error"
	ReqMgrErrorName      = "ReqMgr upstream error"
	RunRegistryErrorName = "RunRegistry upstream error"
	McMErrorName         = "McM upstream error"
	DashboardErrorName   = "Dashboard upstream error"
	SiteDBErrorName      = "SiteDB upstream error"
	CondDBErrorName      = "CondDB upstream error"
	CombinedErrorName    = "Combined error"
	MongoDBErrorName     = "MongoDB error"
	DASProxyErrorName    = "DAS proxy error"
	DASQueryErrorName    = "DAS query error"
)
