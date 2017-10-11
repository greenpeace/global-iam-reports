package reports

import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.srvc.audit.dto.AuditLogTarget
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn

import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.*
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.res.dto.Resource
import org.openiam.idm.srvc.res.service.ResourceDataService
import org.openiam.idm.srvc.user.dto.User
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.*

public class EmployeeCertReport implements ReportDataSetBuilder {

    final static String DEFAULT_REQUESTER_ID = "3000"
    final static Language DEFAULT_LANGUAGE = new Language(id:  1)

    def Set<String> masterKeys = new HashSet<>()

    private ApplicationContext context
    private UserDataWebService userWebService
    private AuditLogService auditLogService
    private ResourceDataService resourceService

    DateFormat dateFormat

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {

        println "EmployeeCertReport data source, request: " + query.queryParams.values().join(", ")
        userWebService = context.getBean("userWS")
        auditLogService = context.getBean(AuditLogService.class)
        resourceService = context.getBean(ResourceDataService.class)

        def String managerId = query.getParameterValue("MANAGER_ID")
        def String employeeId = query.getParameterValue("EMPLOYEE_ID")
        def String periodStartString = query.getParameterValue("PERIOD_START")
        def String periodEndString = query.getParameterValue("PERIOD_END")
        def Date periodStart = dateFromString(periodStartString)
        def Date periodEnd = dateFromString(periodEndString)

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"

        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"
            if (managerId) {
                row.column.add(new ReportColumn('MANAGER', getUserFullName(managerId)))
            }
            if (employeeId) {
                row.column.add(new ReportColumn('EMPLOYEE', getUserFullName(employeeId)))
            }
            if (periodStart) {
                def period = "from " + dateToString(periodStart)
                row.column.add(new ReportColumn('PERIOD_START', periodStart.time.toString()))
                if (periodEnd) {
                    period += " to " + dateToString(periodEnd)
                    row.column.add(new ReportColumn('PERIOD_END', periodEnd.time.toString()))
                }
                row.column.add(new ReportColumn('PERIOD', period))
            }
            reportTable.row.add(row)

        } else {

            reportTable.name = "details"
            def messages = validateParameters(managerId, employeeId, periodStart, periodEnd) as String[]
            if (messages) {
                for(def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                def AuditLogSearchBean logSearchBean = new AuditLogSearchBean()
                logSearchBean.action = "RECERTIFICATION TASK"
                if (periodStart)
                    logSearchBean.from = periodStart
                if (periodEnd)
                    logSearchBean.to = periodEnd
                if (managerId)
                    logSearchBean.userId = managerId
                if (employeeId) {
                    logSearchBean.targetType = "USER"
                    logSearchBean.targetId = employeeId
                }
                logSearchBean.sortBy = [new SortParam(OrderConstants.DESC, "timestamp")] as List
                def auditLogs = auditLogService.findBeans(logSearchBean, 0, 1000)
                for(IdmAuditLog l : auditLogs) {
                    def log = auditLogService.findById(l.id)
                    addMainTableRow(log, reportTable)
                }

                println "EmployeeCertReport data source, response size: " + reportTable.row.size() + " rows"
            }
        }

        return new ReportDataDto( tables : [ reportTable ] as List<ReportTable> )
    }

    private void addMainTableRow(IdmAuditLog a, ReportTable reportTable) {

        final String employeeId = a.targets.find({it.targetType == "USER"})?.targetId
        final String managerId = a.userId

        if (employeeId && managerId) {
            def key = employeeId + "," + managerId
            if (!masterKeys.contains(key)) {
                def employeeName = getUserFullName(employeeId)
                def managerName = getUserFullName(managerId)
                masterKeys.add(key)

                // Find related child workflows
                def auditLogs = a.getChildLogs()

                for(IdmAuditLog l : auditLogs) {
                    final IdmAuditLog log = auditLogService.findById(l.id)
                    final String date = dateToString(log.timestamp)
                    log.targets.each({ target ->
                        def res = getTargetName(target);
                        if (res != null) {
                            ReportRow row = new ReportRow()
                            row.column.add(new ReportColumn('LOG_ID', a.id))
                            row.column.add(new ReportColumn('EMPLOYEE', employeeName))
                            row.column.add(new ReportColumn('MANAGER', managerName))
                            row.column.add(new ReportColumn('LAST_DATE', date))
                            row.column.add(new ReportColumn('RES', res))
                            row.column.add(new ReportColumn('CERTIFY', log.action == "CERTIFY" ? "Y" : "N"))
                            reportTable.row.add(row)
                        }
                    })
                }
            }
        }
    }

    static def validateParameters(String managerId, String employeeId, Date periodStart, Date periodEnd) {
        def violations = [] as List
        if (!periodStart)
            violations.add "Parameter 'Period start' is required"
        if (periodStart && periodEnd && periodEnd < periodStart)
            violations.add "Parameter 'Period start' has to be less than 'Period end'"
        return violations
    }

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    def String getUserFullName(String userId) {
        def User user = userWebService.getUserWithDependent(userId, DEFAULT_REQUESTER_ID, false)
        return user ? (user.firstName +
                (user.middleInit ? ' ' + user.middleInit : '') +
                (user.lastName ? ' ' + user.lastName : '')) : '[ Not found ]';
    }

    def String getTargetName(AuditLogTarget target) {
        if (target.targetType == "RESOURCE") {
            def res = resourceService.getResource(target.targetId, DEFAULT_LANGUAGE)
            if (res.resourceType.id == "MENU_ITEM") {
                return null;
            }
            if (res) {
                return "[" + res.resourceType?.displayName + "] " +
                        (res.displayName ?: res.coorelatedName ?: res.name)
            }
        }
        return "[" + target.targetType + "] " + target.objectPrincipal
    }

    Date dateFromString(String periodStartString) {
        try {
            return dateFormat && periodStartString ? dateFormat.parse(periodStartString) : null
        } catch (ParseException ignored) {
            return null
        }
    }

    String dateToString(Date date) {
        return (date && dateFormat) ? dateFormat.format(date) : ""
    }
}


