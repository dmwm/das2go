### **v04.07.42 to v4.7.42:**


### **v04.07.42rc1 to v04.07.42:**


### **04.07.41 to v04.07.42rc1:**
  - Revert broken Rucio dataset information normalization (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Normalize Rucio Dataset info (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Second attempt to enrich dataset level infromation from Rucio. (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Enrich dataset level infromation from rucion instead of local aggregation. (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Revert broken dataset level aggregatiosn. (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Enrich dataset level information fetched by Rucio && implement block level aggregations per dataset (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Normalize dataset list output from Rucio (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add dataset4dataset map for Rucio bakcend (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add file4block map for rucio backend (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add queries with dataset level lookup granularity and site= filters (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add renderer fall back mechanism for origin_site (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Fix dbs backend site list rendering (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Updating templates and examples (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Enrich the block level infromation fetch from rucio backend reagrdless of the filter applied (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add block4block api and map for rucio backend (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add rucioBlockReplicaInfo in place of rucioBlockAtSite and get more information per block at site (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Fix omitted cleanup steps for mapspush target due to missing clean_*.js files (Todor Ivanov) [#81](https://github.com/dmwm/das2go/pull/81)
  - Fix origin_site map for dbs (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add README_DEVOPS.md (Todor Ivanov) [#79](https://github.com/dmwm/das2go/pull/79)
  - Fix mapspush target (Todor Ivanov) [#79](https://github.com/dmwm/das2go/pull/79)
  - Automating devops procedures through devops.mk file (Todor Ivanov) [#79](https://github.com/dmwm/das2go/pull/79)
  - Add site= filtering for Rucio backend and block lookup key (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Change the DBS filter origin_site to has meaning only on block level granularity (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Add origin_site= filter for the DBS backend (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)
  - Change site lookup key for DBS backend to origin_site (Todor Ivanov) [#77](https://github.com/dmwm/das2go/pull/77)


### **04.07.41rc1 to 04.07.41:**


### **04.07.40 to 04.07.41rc1:**
  - Fix stable tag creation for upload to registry step instead of build image step in build gh/action (Todor Ivanov) [8f9c8d](https://github.com/dmwm/das2go/commit/8f9c8dc194cffbc1c8aa00cfa7bb074b61ce0fa5) on master


### **04.07.40rc3 to 04.07.40:**


### **04.07.40rc2 to 04.07.40rc3:**
  - Fix broken upload_url for upload step in build gh/action (Todor Ivanov) [e420ba](https://github.com/dmwm/das2go/commit/e420baac2f6df9bbeed8f78dec6ec6ef45271da7) on master


### **04.07.40rc1 to 04.07.40rc2:**
  - Separate Release Candidate actions from Stable Release actions (Todor Ivanov) [#72](https://github.com/dmwm/das2go/pull/72)


### **04.07.35 to 04.07.40rc1:**
  - Update XSDB url (Cedric Verstege) [#68](https://github.com/dmwm/das2go/pull/68)
  - Fix X509_USER_PROXY handling (iarspider) [#61](https://github.com/dmwm/das2go/pull/61)
  - fix compilation error (Valentin Kuznetsov) [4f7f90](https://github.com/dmwm/das2go/commit/4f7f90205bdba7e790a044c81e07475428756633) on 
  - Apply validFileOnly to prod only instance (Valentin Kuznetsov) [c175ce](https://github.com/dmwm/das2go/commit/c175cee9db102c22f57a6dc82f708c897661681f) on 
  - update dependencies (Valentin Kuznetsov) [a83b5f](https://github.com/dmwm/das2go/commit/a83b5fcfc333083c1596db240d53bcdf6bef3b3c) on 
  - Adjust DAS web ui to present site block/files info (Valentin Kuznetsov) [d635d6](https://github.com/dmwm/das2go/commit/d635d67d9cb408bf691887acd19efba41ad2be66) on 
  - Add additional info to site dict (Valentin Kuznetsov) [2b260a](https://github.com/dmwm/das2go/commit/2b260a7e57140de2638cfe5789e13cf1a948f0ac) on 
  - Update dependencies (Valentin Kuznetsov) [3e7e06](https://github.com/dmwm/das2go/commit/3e7e063000b8945bba2febe62724c5085b54ccc0) on 
  - Add deep option (Valentin Kuznetsov) [6e74f2](https://github.com/dmwm/das2go/commit/6e74f2ec5c8ddb5558b748c77276b7c2692f9484) on 


