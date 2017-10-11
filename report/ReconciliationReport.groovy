package reports

import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.srvc.audit.dto.IdmAuditLogCustom
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.mngsys.dto.ManagedSysDto
import org.openiam.idm.srvc.mngsys.ws.ManagedSystemWebService
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.user.dto.User
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.DateFormat
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn

class ReconciliationReport implements ReportDataSetBuilder {

    private ApplicationContext context
    private final String DEFAULT_REQUESTER_ID = "3000"

    private AuditLogService auditLogService
    private ManagedSystemWebService managedSystemService
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
        managedSystemService = context.getBean(ManagedSystemWebService.class)

        def listParameter = query.getParameterValue("GET_VALUES")
        if (listParameter) {
            def table = listValues(listParameter)
            return packReportTable(table)
        }

        def String action = query.getParameterValue("ACTION")
        def String managedSysID = query.getParameterValue("MANAGED_SYS_ID")
        def Boolean isDetailsRequest = query.getParameterValue("TABLE") == "DETAILS"

        ManagedSysDto managedSys = managedSystemService.getManagedSys(managedSysID)

        AuditLogSearchBean logSearchBean = new AuditLogSearchBean()
        logSearchBean.action = action
        logSearchBean.targetId = managedSysID
        logSearchBean.targetType = "MANAGED_SYS"
        logSearchBean.sortBy = [new SortParam(OrderConstants.DESC, "timestamp")] as List

        List<IdmAuditLog> auditLogs = auditLogService.findBeans(logSearchBean, 0, 1)

        ReportTable reportTable = new ReportTable()
        if (isDetailsRequest) {
            reportTable.name = "DetailsTable"
            for(IdmAuditLog a : auditLogs) {
                def auditLogCustom = auditLogService.findById(a.id)
                for(IdmAuditLogCustom ch : sortCustomByTime(auditLogCustom.customRecords)) {
                    addDetailsRow(ch, reportTable)
                }
            }
        } else {
            reportTable.name = "HeadTable"
            for(IdmAuditLog a : auditLogs) {
                addHeadRow(a, managedSys, reportTable)
            }
        }

        return packReportTable(reportTable)
    }

    static def sortCustomByTime(Collection<IdmAuditLogCustom> collection) {
        def records = [] as List<IdmAuditLogCustom>
        records.addAll(collection)
        return records.sort({ x,y -> x.timestamp == y.timestamp ? x.id <=> y.id : x.timestamp <=> y.timestamp })
    }
    
    private void addHeadRow(IdmAuditLog auditLog, ManagedSysDto managedSys, ReportTable reportTable) {
        ReportRow row = new ReportRow()
        row.column.add(new ReportColumn('LOG_ID', auditLog.id))
        row.column.add(new ReportColumn('ACTION_ID', auditLog.action))
        row.column.add(new ReportColumn('ACTION_STATUS', auditLog.result))
        row.column.add(new ReportColumn('ACTION_DATETIME', dateTimeFormat.format(auditLog.timestamp)))
        row.column.add(new ReportColumn('USER_ID', auditLog.userId))
        row.column.add(new ReportColumn('USER_FULLNAME', getUserFullName(auditLog.userId)))
        row.column.add(new ReportColumn('LOGIN_ID', auditLog.principal))
        row.column.add(new ReportColumn('HOST', auditLog.nodeIP))
        row.column.add(new ReportColumn('CLIENT_ID', auditLog.clientIP))
        row.column.add(new ReportColumn('TARGET_SYSTEM_ID', auditLog.managedSysId))
        row.column.add(new ReportColumn('SESSION_ID', auditLog.sessionID))
        row.column.add(new ReportColumn('MANAGED_SYS_NAME', managedSys?.name))
        row.column.add(new ReportColumn('MANAGED_SYS_DESCRIPTION', managedSys?.description))
        reportTable.row.add(row)
    }

    private void addDetailsRow(IdmAuditLogCustom a, ReportTable reportTable) {
        ReportRow row = new ReportRow()
        row.column.add(new ReportColumn('LOG_ID', a.id))
        row.column.add(new ReportColumn('ACTION_DATETIME', a.timestamp.toString()))
        row.column.add(new ReportColumn('FORMATTED_DATETIME', dateTimeFormat.format(new Date(a.timestamp))))
        row.column.add(new ReportColumn('LOG_CUSTOM_KEY', a.key))
        row.column.add(new ReportColumn('LOG_CUSTOM_VALUE', a.value))
        reportTable.row.add(row)
    }

    private ReportTable listValues(String parameter) {
        ReportTable reportTable = new ReportTable()
        reportTable.name = "values"

        def ReportRow row
        switch(parameter) {
            case "MANAGED_SYS":
                for(ManagedSysDto managedSys : managedSystemService.getAllManagedSys()) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('MANAGED_SYS_ID', managedSys.id))
                    row.column.add(new ReportColumn('MANAGED_SYS_NAME', managedSys.name))
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