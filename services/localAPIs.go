package services

// LocalAPIs structure to hold information about local APIs
type LocalAPIs struct{}

// LocalAPIMap contains a map of local APIs and their associative functions
func LocalAPIMap() map[string]string {
	localAPIMap := make(map[string]string)
	localAPIMap["combined_dataset4site_release"] = "Dataset4SiteRelease"
	localAPIMap["combined_dataset4site_release_parent"] = "Dataset4SiteReleaseParent"
	localAPIMap["combined_child4site_release_dataset"] = "Child4SiteReleaseDataset"
	localAPIMap["combined_site4block"] = "Site4Block"
	localAPIMap["combined_site4dataset"] = "Site4Dataset"
	localAPIMap["combined_lumi4dataset"] = "Lumi4Dataset"
	localAPIMap["combined_files4dataset_runs_site"] = "Files4DatasetRunsSite"
	localAPIMap["combined_files4block_runs_site"] = "Files4BlockRunsSite"
	localAPIMap["dbs3_dataset4block"] = "Dataset4Block"
	localAPIMap["dbs3_run_lumi4dataset"] = "RunLumi4Dataset"
	localAPIMap["dbs3_run_lumi_evts4dataset"] = "RunLumiEvents4Dataset"
	localAPIMap["dbs3_run_lumi4block"] = "RunLumi4Block"
	localAPIMap["dbs3_run_lumi_evts4block"] = "RunLumiEvents4Block"
	localAPIMap["dbs3_file_lumi4dataset"] = "FileLumi4Dataset"
	localAPIMap["dbs3_file_lumi_evts4dataset"] = "FileLumiEvents4Dataset"
	localAPIMap["dbs3_file_lumi4block"] = "FileLumi4Block"
	localAPIMap["dbs3_file_lumi_evts4block"] = "FileLumiEvents4Block"
	localAPIMap["dbs3_file_run_lumi4dataset"] = "RunLumi4Dataset"
	localAPIMap["dbs3_file_run_lumi_evts4dataset"] = "RunLumiEvents4Dataset"
	localAPIMap["dbs3_file_run_lumi4block"] = "FileRunLumi4Block"
	localAPIMap["dbs3_file_run_lumi_evts4block"] = "RunLumiEvents4Block"
	localAPIMap["dbs3_block_run_lumi4dataset"] = "RunLumi4Dataset"
	localAPIMap["dbs3_file4dataset_run_lumi"] = "File4DatasetRunLumi"
	localAPIMap["dbs3_blocks4tier_dates"] = "Blocks4TierDates"
	localAPIMap["dbs3_lumi4block_run"] = "Lumi4BlockRun"
	localAPIMap["dbs3_datasetlist"] = "DatasetList"
	localAPIMap["reqmgr2_configs"] = "Configs"
	localAPIMap["sitedb2_site_names"] = "SiteNames"
	localAPIMap["sitedb2_groups"] = "Groups"
	localAPIMap["sitedb2_group_responsibilities"] = "GroupResponsibilities"
	localAPIMap["sitedb2_people_via_email"] = "PeopleEmail"
	localAPIMap["sitedb2_people_via_name"] = "PeopleName"
	localAPIMap["sitedb2_roles"] = "Roles"
	return localAPIMap
}

// DASLocalAPIs contains list of __ONLY__ exceptional apis due to mistake in DAS maps
func DASLocalAPIs() []string {
	out := []string{
		// dbs3 APIs which should be treated as local_api, but they have
		// url: http://.... in their map instead of local_api
		"file_run_lumi4dataset", "file_run_lumi4block",
		"file_run_lumi_evts4dataset", "file_run_lumi_evts4block",
		"run_lumi_evts4dataset", "file_lumi_evts4dataset",
		"file_lumi4dataset", "file_lumi4block", "run_lumi4dataset", "run_lumi4block",
		"block_run_lumi4dataset", "file4dataset_run_lumi", "blocks4tier_dates",
		"lumi4block_run", "datasetlist", "configs"}
	return out
}
