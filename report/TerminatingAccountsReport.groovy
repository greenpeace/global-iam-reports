package org.openiam.connector

import org.openiam.base.OrderConstants
import org.openiam.base.ws.SortParam
import org.openiam.idm.searchbeans.AuditLogSearchBean
import org.openiam.idm.srvc.audit.constant.AuditAction
import org.openiam.idm.srvc.audit.dto.IdmAuditLog
import org.openiam.idm.srvc.audit.service.AuditLogService
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn
import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.ReportDataSetBuilder
import org.openiam.idm.srvc.user.domain.UserEntity
import org.openiam.idm.srvc.user.dto.UserStatusEnum
import org.openiam.idm.srvc.user.service.UserDataService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import java.text.DateFormat
import java.text.ParseException

public class TerminatingAccountsReport implements ReportDataSetBuilder {

    final static String NOT_FOUND = '[ Not found ]'
    final static int LOGS_LIMIT = 10000

    private ApplicationContext context
    private UserDataService userDataService
    private OrganizationDataService organizationService
    private AuditLogService auditLogService
    private Language DEFAULT_LANGUAGE = new Language(id: 1)

    DateFormat dateFormat

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }

    @Override
    ReportDataDto getReportData(ReportQueryDto query) {

        println "TerminatingAccounts data source, request: " + query.queryParams.values().join(", ")
        userDataService = context.getBean("userManager") as UserDataService
        organizationService = context.getBean("orgManager") as OrganizationDataService
        auditLogService = context.getBean(AuditLogService.class)

        def String orgId = query.getParameterValue("ORG_ID")
        def String periodStartString = query.getParameterValue("PERIOD_START")
        def String periodEndString = query.getParameterValue("PERIOD_END")
        def Date periodStart = dateFromString(periodStartString)
        def Date periodEnd = dateFromString(periodEndString)

        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"
        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.name = "head"
            if (orgId) {
                def Organization bean = organizationService.getOrganizationLocalized(orgId, null, DEFAULT_LANGUAGE)
                row.column.add(new ReportColumn('ORGANIZATION', bean?.name ?: NOT_FOUND))
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

            reportTable.setName("details")

            def messages = validateParameters(orgId, periodStart, periodEnd) as String[]
            if (messages) {
                for(def msg : messages) {
                    def ReportRow row = new ReportRow()
                    row.column.add(new ReportColumn('ERROR', msg))
                    reportTable.row.add(row)
                }
            } else {

                def rows = new HashMap<String, ReportRow>()

                def searchBean = new AuditLogSearchBean(
                        from: periodStart,
                        to: periodEnd,
                        sortBy: [new SortParam(OrderConstants.ASC, "timestamp")] as List
                )
                searchBean.action = AuditAction.USER_DEACTIVATE.value()
                processLogs(searchBean, orgId, rows)
                searchBean.action = AuditAction.USER_DISABLE.value()
                processLogs(searchBean, orgId, rows)
                searchBean.action = AuditAction.PROVISIONING_DISABLE.value()
                processLogs(searchBean, orgId, rows)

                rows.values().each { reportTable.row.add(it) }
                println "TerminatingAccounts data source, response size: " + reportTable.row.size() + " rows"
            }
        }
        return packReportTable(reportTable)
    }

    void processLogs(AuditLogSearchBean searchBean, String orgId, Map<String, ReportRow> rows) {
        def actionName = getActionName(searchBean.action)
        def logIds = auditLogService.findIDs(searchBean, 0, LOGS_LIMIT)
        for (def logId : logIds) {
            IdmAuditLog log = auditLogService.findById(logId)
            def targetUserId = log.targets.find({ it.targetType == "USER" })?.targetId
            if (!targetUserId)
                continue
            def user = userDataService.getUser(targetUserId)
            if (!user)
                continue
            if (orgId && !user.organizationUser.find({ it.organization?.id == orgId }))
                continue

            def final key = actionName + ',' + user.id
            if (rows.containsKey(key)) {
                // process younger (more actual) records
                def lastTime = rows.get(key).column.get(0).value.toLong()
                def newTime = log.timestamp?.time
                if (newTime <= lastTime)
                    continue
            }

            def row = new ReportRow()
            row.column.add(new ReportColumn('ACTION_DATE', log.timestamp?.time?.toString()))
            row.column.add(new ReportColumn('FULL_NAME', user.userAttributes.get("FULL_NAME")?.value ?:
                    user.firstName + ' ' + (user.middleInit ? user.middleInit + ' ' : '') + user.lastName))
            row.column.add(new ReportColumn('TITLE', user.title))
            row.column.add(new ReportColumn('EMPLOYEE_ID', user.employeeId))
            row.column.add(new ReportColumn('DEPARTMENT', user.getOrganizationUser().find({ true })?.organization?.name))
            row.column.add(new ReportColumn('CURRENT_STATUS', getUserStatus(user)))
            row.column.add(new ReportColumn('ACTION_NAME', actionName))

            rows.put(key, row)
        }
    }

    static String getUserStatus(UserEntity user) {
        return (user.status != UserStatusEnum.DELETED &&
                user.status != UserStatusEnum.TERMINATED &&
                user.secondaryStatus == UserStatusEnum.DISABLED) ?
                UserStatusEnum.DISABLED.value : user.status?.value
    }

    static String getActionName(String action) {
        return action == AuditAction.USER_DEACTIVATE.value() ? "DEACTIVATE" :
               action == AuditAction.USER_DISABLE.value() ? "DISABLE" :
               action == AuditAction.PROVISIONING_DISABLE.value() ? "DISABLE" :
               "TERMINATED"
    }

    static def validateParameters(String orgId, Date periodStart, Date periodEnd) {
        def violations = [] as List
        if (!periodStart)
            violations.add "Parameter 'Period start' is required"
        if (periodStart && periodEnd && periodEnd < periodStart)
            violations.add "Parameter 'Period start' has to be less than 'Period end'"
        return violations
    }

    static ReportDataDto packReportTable(ReportTable reportTable)
    {
        ReportDataDto reportDataDto = new ReportDataDto()
        List<ReportTable> reportTables = new ArrayList<ReportTable>()
        reportTables.add(reportTable)
        reportDataDto.setTables(reportTables)
        return reportDataDto
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

