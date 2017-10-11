package reports

import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.srvc.audit.dto.IdmAuditLogCustom
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.synch.dto.SynchConfig
import org.openiam.idm.srvc.synch.ws.IdentitySynchWebService
import org.openiam.idm.srvc.user.dto.User
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.DateFormat
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn

class SynchronizationReport implements ReportDataSetBuilder {

    private ApplicationContext context
    private final String DEFAULT_REQUESTER_ID = "3000"

    AuditLogService auditLogService
    IdentitySynchWebService identitySynchService
    private UserDataWebService userWebService

    DateFormat dateTimeFormat

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    @Override
    ReportDataDto getReportData(ReportQueryDto query) {

        userWebService = context.getBean("userWS")
        auditLogService = context.getBean(AuditLogService.class)
        identitySynchService = context.getBean(IdentitySynchWebService.class)

        if (query.getParameterValue("GET_VALUES")) {
            def table = listValues(query)
            return packReportTable(table)
        }

        def String action = query.getParameterValue("ACTION")
        def String configId = query.getParameterValue("CONFIG_ID")
        def String logId = query.getParameterValue("AUDIT_LOG_ID")
        def Boolean isDetailsRequest = query.getParameterValue("TABLE") == "DETAILS"

        SynchConfig synchConfig = identitySynchService.findById(configId).config

        AuditLogSearchBean logSearchBean = new AuditLogSearchBean()
        logSearchBean.action = action
        logSearchBean.source = configId
        logSearchBean.sortBy = [new SortParam(OrderConstants.DESC, "timestamp")] as List

        if (logId) {
            logSearchBean.key = logId
        }

        List<IdmAuditLog> auditLogResords = auditLogService.findBeans(logSearchBean, 0, 1)

        ReportTable reportTable = new ReportTable()

        if (isDetailsRequest) {
            reportTable.name = "DetailsTable"
            for(IdmAuditLog a : auditLogResords) {
                def IdmAuditLog log = auditLogService.findById(a.id)
                for(IdmAuditLogCustom r : sortCustomByTime(log.customRecords)) {
                    addDetailRow(r, reportTable, log)
                }

                for(IdmAuditLog ch : sortLogByTime(log.childLogs)) {
                    def IdmAuditLog chLog = auditLogService.findById(ch.id)
                    for(IdmAuditLogCustom r : sortCustomByTime(chLog.customRecords)) {
                        addDetailRow(r, reportTable, chLog)
                    }
                }
            }
        } else {
            reportTable.name = "HeadTable"
            for(IdmAuditLog a : sortLogByTime(auditLogResords)) {
                addHeadRow(a, synchConfig, reportTable, null)
                def IdmAuditLog log = auditLogService.findById(a.id)
                for(IdmAuditLog ch : sortLogByTime(log.childLogs)) {
                    addHeadRow(ch, synchConfig, reportTable, a.id)
                }
            }
        }

        return packReportTable(reportTable)
    }

    static def sortCustomByTime(Collection<IdmAuditLogCustom> collection) {
        def records = [] as List<IdmAuditLogCustom>
        records.addAll(collection)
        return records.sort({ x,y -> x.timestamp == y.timestamp ? x.id <=> y.id : x.timestamp <=> y.timestamp })
    }

    static def sortLogByTime(Collection<IdmAuditLog> collection) {
        def records = [] as List<IdmAuditLog>
        records.addAll(collection)
        return records.sort({ x,y -> x.timestamp == y.timestamp ? x.id <=> y.id : x.timestamp <=> y.timestamp })
    }

    private void addHeadRow(IdmAuditLog auditLog, SynchConfig synchConfig, ReportTable reportTable, String parentActionId) {
        ReportRow row = new ReportRow()
        row.column.add(new ReportColumn('LOG_ID', auditLog.id))
        row.column.add(new ReportColumn('ACTION_ID', auditLog.action))
        row.column.add(new ReportColumn('PARENT_ACTION_ID', parentActionId))
        row.column.add(new ReportColumn('ACTION_STATUS', auditLog.result))
        row.column.add(new ReportColumn('ACTION_DATETIME', dateTimeFormat.format(auditLog.timestamp)))
        row.column.add(new ReportColumn('USER_ID', auditLog.userId))
        row.column.add(new ReportColumn('USER_FULLNAME', getUserFullName(auditLog.userId)))
        row.column.add(new ReportColumn('LOGIN_ID', auditLog.principal))
        row.column.add(new ReportColumn('HOST', auditLog.nodeIP))
        row.column.add(new ReportColumn('CLIENT_IP', auditLog.clientIP))
        row.column.add(new ReportColumn('TARGET_SYSTEM_ID', auditLog.managedSysId))
        row.column.add(new ReportColumn('SESSION_ID', auditLog.sessionID))
        if (synchConfig) {
            row.column.add(new ReportColumn('CONFIG_NAME', synchConfig.name))
        }
        reportTable.row.add(row)
    }

    private void addDetailRow(IdmAuditLogCustom a, ReportTable reportTable, IdmAuditLog parentAction) {
        ReportRow row = new ReportRow()
        row.column.add(new ReportColumn('LOG_ID', a.id))
        row.column.add(new ReportColumn('PARENT_ACTION_ID', parentAction.id))
        row.column.add(new ReportColumn('PARENT_ACTION', parentAction.action))
        row.column.add(new ReportColumn('ACTION_DATETIME', a.timestamp.toString()))
        row.column.add(new ReportColumn('FORMATTED_DATETIME', dateTimeFormat.format(new Date(a.timestamp))))
        row.column.add(new ReportColumn('LOG_CUSTOM_KEY', a.key))
        row.column.add(new ReportColumn('LOG_CUSTOM_VALUE', a.value))
        reportTable.row.add(row)
    }

    private ReportTable listValues(ReportQueryDto query) {
        ReportTable reportTable = new ReportTable()
        reportTable.name = "values"

        def ReportRow row
        switch(query.getParameterValue("GET_VALUES")) {
            case "CONFIGURATION":
                def listResponse = identitySynchService.getAllConfig()
                for(SynchConfig config : listResponse.configList) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('CONFIG_ID', config.synchConfigId))
                    row.column.add(new ReportColumn('CONFIG_NAME', config.name))
                    reportTable.row.add(row)
                }
                break
            case "AUDIT_LOG":
                final String configId = query.getParameterValue("CONFIG_ID")
                AuditLogSearchBean logSearchBean = new AuditLogSearchBean()
                logSearchBean.action = "SYNCHRONIZATION"
                logSearchBean.source = configId
                logSearchBean.sortBy = [new SortParam(OrderConstants.DESC, "timestamp")] as List

                List<IdmAuditLog> entities = auditLogService.findBeans(logSearchBean, 0, 50)
                for(IdmAuditLog auditLog : entities) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('AUDIT_LOG_ID', auditLog.id))
                    row.column.add(new ReportColumn('AUDIT_LOG_NAME', dateTimeFormat.format(auditLog.timestamp)))
                    reportTable.row.add(row)
                }
                break
        }
        return reportTable
    }

    private String getUserFullName(String userId) {
        def User user = userWebService.getUserWithDependent(userId, DEFAULT_REQUESTER_ID, false)
        return user ? (user.firstName +
                (user.middleInit ? ' ' + user.middleInit : '') +
                (user.lastName ? ' ' + user.lastName : '')) : '';
    }

    private static ReportDataDto packReportTable(ReportTable reportTable) {
        return new ReportDataDto(
            tables : [ reportTable ] as List<ReportTable>
        )
    }
}
