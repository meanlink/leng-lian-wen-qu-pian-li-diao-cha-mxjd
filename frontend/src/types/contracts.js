/** @typedef {{id:number,name:string,source:string,rows:number,checksum:string}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,status:string,result:Object,approved:boolean}} AnalysisJob */
/** @typedef {{id:number,name:string,job_id:number,destination:string,next_run:number,enabled:boolean}} ReportSchedule */
/** @typedef {{id:number,name:string,status:string,row_total:number,artifact_count:number,size_bytes:number,created_at:number,generation:number,note:string}} Backup */
/** @typedef {{id:number,backup_id:number,status:string,stage:string,progress:number,message:string,error:string,verification:Object,result:Object,rollback_available:boolean,attempt:number,mode:string}} Restore */
/** @typedef {{generation:number,maintenance:boolean,maintenance_reason:string,active_restore_id:number|null,backup_active:boolean}} OpsStatus */
export {};
