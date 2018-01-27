package reports

import org.openiam.idm.searchbeans.EmailSearchBean
import org.openiam.idm.searchbeans.MetadataTypeSearchBean
import org.openiam.idm.searchbeans.OrganizationSearchBean
import org.openiam.idm.searchbeans.PhoneSearchBean
import org.openiam.idm.searchbeans.RoleSearchBean
import org.openiam.idm.searchbeans.UserSearchBean
import org.openiam.idm.srvc.auth.dto.Login
import org.openiam.idm.srvc.auth.ws.LoginDataWebService
import org.openiam.idm.srvc.continfo.domain.EmailAddressEntity
import org.openiam.idm.srvc.continfo.dto.EmailAddress
import org.openiam.idm.srvc.lang.dto.Language
import org.openiam.idm.srvc.meta.dto.MetadataType
import org.openiam.idm.srvc.meta.ws.MetadataWebService
import org.openiam.idm.srvc.meta.domain.MetadataTypeEntity
import org.openiam.idm.srvc.org.service.OrganizationDataService
import org.openiam.idm.srvc.continfo.domain.PhoneEntity
import org.openiam.idm.srvc.org.dto.Organization
import org.openiam.idm.srvc.report.dto.ReportQueryDto
import org.openiam.idm.srvc.report.dto.ReportRow
import org.openiam.idm.srvc.report.dto.ReportRow.ReportColumn

import org.openiam.idm.srvc.report.dto.ReportTable
import org.openiam.idm.srvc.report.service.*
import org.openiam.idm.srvc.report.dto.ReportDataDto
import org.openiam.idm.srvc.role.dto.Role
import org.openiam.idm.srvc.role.ws.RoleDataWebService
import org.openiam.idm.srvc.user.domain.UserEntity
import org.openiam.idm.srvc.user.service.UserDataService
import org.openiam.idm.srvc.user.domain.UserAttributeEntity
import org.openiam.idm.srvc.user.ws.UserDataWebService
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext

import com.google.gdata.data.appsforyourdomain.EmailList

import java.text.*

public class GpUserReport implements ReportDataSetBuilder {

	static final String DEFAULT_REQUESTER_ID = "3000"
	static final Language DEFAULT_LANGUAGE = new Language(id:  1)
    static final String STATUS_META_GROUP = "USER"
    static final String SEC_STATUS_META_GROUP = "USER_2ND_STATUS"
	static final String NRO_TYPE_ID = "402894ad50f651a10150f66501410049"
	static final String NRO_OFFICE_TYPE_ID = "402894ad50f651a10150f665d78f004e"
	final static String ScriptName = "GpUserReport"

    private ApplicationContext context
    private UserDataService userDataService
	private UserDataWebService userDataWebService
    private OrganizationDataService organizationService
    private RoleDataWebService roleDataWebService
    private LoginDataWebService loginDataWebService
    private Language language = getDefaultLanguage()

    DateFormat dateFormat

    @Override
    public ReportDataDto getReportData(ReportQueryDto query) {
		
		println("=== $ScriptName Start")
        println "=== $ScriptName data source, request: " + query.queryParams.values().join(", ")
		
        userDataService = context.getBean("userManager")
		userDataWebService = context.getBean("userWS")
        loginDataWebService = context.getBean("loginWS")
        metadataWebService = context.getBean("metadataWS")
        organizationService = context.getBean("orgManager")
        roleDataWebService = context.getBean("roleWS")

        def listParameter = query.getParameterValue("GET_VALUES")
        if (listParameter) {
            def table = listValues(listParameter)
            return packReportTable(table)
        }

        def String status = query.getParameterValue("STATUS")
		println("=== $ScriptName ParameterValue of STATUS: $status")
		
        def String secStatus = query.getParameterValue("SECONDARY_STATUS")
		println("=== $ScriptName ParameterValue of SECONDARY_STATUS: $secStatus")
		
        def String[] roles = query.getParameterValues("ROLE")
		println("=== $ScriptName ParameterValue of ROLE: " + roles)
		
        def String[] organizations = query.getParameterValues("ORG_ID")
		println("=== $ScriptName ParameterValue of ORG_ID: " + organizations)
		
        def ReportTable reportTable = new ReportTable()

        def isHeadRequest = query.getParameterValue("TABLE") == "HEAD"

        if (isHeadRequest) {

            def ReportRow row = new ReportRow()
            reportTable.setName("head")
            if (status) {
                def bean = getMetadataTypesByGrouping(STATUS_META_GROUP, status)?.get(0)
                if (bean?.displayName) {
                    row.column.add(new ReportColumn('STATUS', bean.displayName))
                }
            }
            if (secStatus) {
                def bean = getMetadataTypesByGrouping(SEC_STATUS_META_GROUP, secStatus)?.get(0)
                if (bean?.displayName) {
                    row.column.add(new ReportColumn('SECONDARY_STATUS', bean.displayName))
                }
            }
            if (!EmptyMultiValue(organizations)) {
				println("=== $ScriptName EmptyMultiValue: " + organizations)
                def String names = ""
                for(def String orgId : organizations) {
                    def Organization bean = organizationService.getOrganizationLocalized(orgId, null, language)
					println("=== $ScriptName orgId: $orgId")
                    if (bean?.name) {
                        names += (names ? ", " : "") + bean.name
                    }
                }
                if (names) {
                    row.column.add(new ReportColumn('ORG_ID', names))
                }
            }
            if (!EmptyMultiValue(roles)) {
                def String names = ""
                for(def String roleId : roles) {
                    def Role bean = roleDataWebService.getRole(roleId, null)
                    if (bean?.name) {
                        names += (names ? ", " : "") + bean.name
                    }
                }
                if (names) {
                    row.column.add(new ReportColumn('ROLE', names))
                }
            }
            reportTable.row.add(row)

        } else {
			reportTable.setName("GPUserReport")
			
			//def messages = validateParameters(organizations, roles, status, secStatus)
			//println("=== $ScriptName validateParameters, responce: $messages")
			
			UserSearchBean searchBean = new UserSearchBean()
				
			if (status) {
				searchBean.setUserStatus(status)
			}
				
			if (secStatus) {
				searchBean.setAccountStatus(secStatus)
			}
				
			if (!EmptyMultiValue(organizations)) {
				Set<String> orgIds = new HashSet<String>()
				for(def String orgId : organizations) {
					orgIds.add(orgId)
				}
				searchBean.addOrganizationIdList(orgIds)
			}
				
			if (!EmptyMultiValue(roles)) {
				def roleIds = new HashSet<String>() as Set<String>
				for(def String roleId : roles) {
					roleIds.add(roleId)
				}
				searchBean.setRoleIdSet(roleIds)
			}
				
			def users = userDataService.getByExample(searchBean, 0, Integer.MAX_VALUE) as List<UserEntity>

			for (UserEntity user : users) {
				def userId = user.id
				List<Organization> NroOfficeList = organizationService.getOrganizationsForUserByTypeLocalized(userId, "300", NRO_OFFICE_TYPE_ID, DEFAULT_LANGUAGE)					
				Map<String,UserAttributeEntity> UserAttributes = user?.getUserAttributes()
				Set<EmailAddressEntity> EmailAddressList = user?.getEmailAddresses()					
				String SecondOfficeId = getUserAttribute(UserAttributes, "Secondary office")					
				Organization ContractedOffice = NroOfficeList.findAll { it.id != SecondOfficeId }[0]
				String EmailType = "Default" 
				def ReportRow row = new ReportRow()
				row.column.add(new ReportColumn('FIRST_NAME', user.firstName))
				println("=== $ScriptName FIRST_NAME: ${user.firstName}")
				
				row.column.add(new ReportColumn('MIDDLE_INIT', user.middleInit))
				println("=== $ScriptName MIDDLE_INIT: ${user.middleInit}")
				
				row.column.add(new ReportColumn('LAST_NAME', user.lastName))
				println("=== $ScriptName LAST_NAME: ${user.lastName}")
				
				row.column.add(new ReportColumn('TITLE', user.title))
				println("=== $ScriptName TITLE: ${user.title}")
				
				row.column.add(new ReportColumn('CONTRACTED_OFFICE', getOfficeNamesByOfficeId(ContractedOffice?.id)))
				println("=== $ScriptName CONTRACTED_OFFICE: ${ContractedOffice?.id}")
				
				row.column.add(new ReportColumn('SECONDARY_OFFICE', getOfficeNamesByOfficeId(SecondOfficeId)))
				println("=== $ScriptName SECONDARY_OFFICE: $SecondOfficeId")
				
				row.column.add(new ReportColumn('STATUS', user.status?.value))
				println("=== $ScriptName STATUS: ${user.status?.value}")
				
				row.column.add(new ReportColumn('SECONDARY_STATUS', user.secondaryStatus?.value))
				println("=== $ScriptName SECONDARY_STATUS: ${user.secondaryStatus?.value}")
				
				row.column.add(new ReportColumn('EMPLOYEE_TYPE', user.getEmployeeType()?.description))
				println("=== $ScriptName EMPLOYEE_TYPE: ${user.getEmployeeType()?.description}")
				
				row.column.add(new ReportColumn('EMPLOYEE_ID', user.employeeId))
				println("=== $ScriptName EMPLOYEE_ID: ${user.employeeId}")
				
				row.column.add(new ReportColumn('EMAIL_ADDRESS', getEmail(EmailAddressList, EmailType)))
				println("=== $ScriptName EMAIL_ADDRESS: " + getEmail(EmailAddressList, EmailType))
				
				row.column.add(new ReportColumn('PHONE', getDefaultPhone(userId)))
				println("=== $ScriptName PHONE: "+ getDefaultPhone(userId))
				
				row.column.add(new ReportColumn('CAMPAIGN', getUserAttribute(UserAttributes, "Campaign")))
				println("=== $ScriptName CAMPAIGN: " + getDefaultPhone(userId))
				
				row.column.add(new ReportColumn('SKYPE', getUserAttribute(UserAttributes, "Skype")))
				println("=== $ScriptName SKYPE: " + getDefaultPhone(userId))
				
				Login l = loginDataWebService.getPrimaryIdentity(userId)?.principal
				
				row.column.add(new ReportColumn('LAST_LOGIN', l?.lastLogin ? dateFormat.format(l.lastLogin) : null))
				//println("=== $ScriptName LAST_LOGIN: " + dateFormat.format(l.lastLogin) ?: "")
				
				row.column.add(new ReportColumn('LOGIN', l?.login))
				println("=== $ScriptName LOGIN: ${l?.login}")
				
				row.column.add(new ReportColumn('MANAGER', getSupervisorNameyUserId(userId)))
				println("=== $ScriptName MANAGER: " + getSupervisorNameyUserId(userId))
				
				reportTable.row.add(row)
			}
        }

        println("=== $ScriptName data source, responce size: " + reportTable.row.size() + " rows")
        return packReportTable(reportTable)
    }
	
	def EmptyMultiValue(String[] values) {
		return (!values || values.length == 1 && !values[0])
	}
	
	private validateParameters(String[] organizationIds, String[] roles, String status, String secStatus) {
		def violations = [] as List
		if (EmptyMultiValue(organizationIds)) {
			violations.add "Parameter 'NRO office' is required"
		} else {
			for(def String orgId : organizationIds) {
				def Organization organization = organizationService.getOrganizationLocalized(orgId, null, language)
				if (organization?.organizationTypeId) {
					if (organization.organizationTypeId != NRO_OFFICE_TYPE_ID) {
						violations.add "Only NRO offices are accepted for parameter 'NRO office'. ${organization.name} is not an NRO Office."
					}
				}
			}
		} 
		println("=== $ScriptName violations: $violations")
		return violations
	}

    private MetadataWebService metadataWebService
	

	private String getUserAttribute(Map<String,UserAttributeEntity> UserAttributes, String AttributeName) {
		def UserAttributeValue = UserAttributes?.get(AttributeName)?.value
		return UserAttributeValue
		}

	private String getOfficeNamesByOfficeId(String NroOfficeId) {
		println("=== $ScriptName getOfficeNamesByOfficeId: NroOfficeId: $NroOfficeId")
		if (NroOfficeId) {
			Organization NroOffice = organizationService.getOrganizationLocalized(NroOfficeId, DEFAULT_REQUESTER_ID, DEFAULT_LANGUAGE)
			List<Organization> Nro = organizationService.getParentOrganizationsLocalized(NroOfficeId, DEFAULT_REQUESTER_ID, 0, 1, DEFAULT_LANGUAGE)
			String NroName = Nro.get(0).name
			String OfficeName = NroOffice.name
			return NroName + "\n" + OfficeName
		} else {
			return ""
		}
	}
		
    private String getCompanyNamesByUserId(String userId) {
        def orgs = organizationService.getOrganizationsForUser(userId, "3000", 0, 100) as List<Organization>
        orgs.sort(true, { a,b -> a.id <=> b.id })
        def result = []
        orgs.each { result += it.abbreviation ?: it.name }
        return result.join(', ')
    }

	private String getSupervisorNameyUserId(String userId) {
	def superiors = userDataWebService.getSuperiors(userId, 0, Integer.MAX_VALUE)
		if (superiors) {
			def supervisor = superiors.get(0)
			def Accounts = userDataWebService.findBeans(new UserSearchBean(key: supervisor.id), 0, 1)
			def supervisorAccount = Accounts ? Accounts.get(0) : null
			def supervisorName = supervisorAccount.firstName + " " +supervisorAccount.lastName
			return supervisorName
		}
	}
    private ReportTable listValues(String parameter) {

        ReportTable reportTable = new ReportTable()
        reportTable.setName("values")

        def ReportRow row

        switch(parameter) {
            case "STATUS":
            case "SECONDARY_STATUS":
                def String metaParameter = parameter == "STATUS" ? STATUS_META_GROUP : SEC_STATUS_META_GROUP
                def List<MetadataType> metadataTypes = getMetadataTypesByGrouping(metaParameter, null)
                for(MetadataType meta : metadataTypes) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('ID', meta.id))
                    row.column.add(new ReportColumn('NAME', meta.displayName))
                    reportTable.row.add(row)
                }
                break

            case "ORGANIZATION":
                def searchBean = new OrganizationSearchBean(deepCopy: false)
                def List<Organization> organizations = organizationService.findBeansLocalized(searchBean, null, 0, Integer.MAX_VALUE, language)
                for(Organization bean : organizations) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('ID', bean.id))
                    row.column.add(new ReportColumn('NAME', bean.name))
                    reportTable.row.add(row)
                }
                break

            case "ROLE":
                def searchBean = new RoleSearchBean(deepCopy: false)
                def List<Role> roles = roleDataWebService.findBeans(searchBean, null, 0, Integer.MAX_VALUE)
                for(Role bean : roles) {
                    row = new ReportRow()
                    row.column.add(new ReportColumn('ID', bean.id))
                    row.column.add(new ReportColumn('NAME', bean.name))
                    reportTable.row.add(row)
                }
                break

        }
        return reportTable
    }

    private List<MetadataType> getMetadataTypesByGrouping(final String grouping, final String id) {
        def MetadataTypeSearchBean searchBean = new MetadataTypeSearchBean()
        searchBean.grouping = grouping
        if (id) {
            searchBean.addKey(id)
        }
        searchBean.active = true
        return metadataWebService.findTypeBeans(searchBean, 0, Integer.MAX_VALUE, language)
    }

    private String getDefaultPhone(String userId) {
        def phones = userDataService.getPhoneList(new PhoneSearchBean(parentId: userId), 100, 0) as List<PhoneEntity>
        def phone = phones ? phones.sort(true, { a,b -> a.isDefault <=> b.isDefault }).get(0) : null
        if (phone) {
            def country = phone.countryCd?.trim() ?: ''
            if (country && !country.startsWith('+')) {
                country = '+' + country
            }
            def area = phone.areaCd ? ('(' + phone.areaCd + ')') : (country ? '-' : '')
            return country + area + (phone.phoneNbr ?: '') + (phone.phoneExt ? ' +' + phone.phoneExt : '')
        }
        return ''
    }

	private String getEmail( Set<EmailAddressEntity> EmailAddressList, String EmailType) {
		def EmailAddress = ""
		if (EmailAddressList.size() > 0) {
			switch (EmailType){
				case "Default" :
					EmailAddress =  EmailAddressList.find{it.getIsDefault()}.getEmailAddress() ?: "Not Default Email Set"
					break
				default:
					EmailAddress = EmailAddressList[0].getEmailAddress()
				break
			}
		} else {
			EmailAddress = "Account has no Emaill Address"
		}
		return EmailAddress
	}
	
    private String getDefaultEmail(String userId) {
        def emails = userDataService.getEmailAddressList(new EmailSearchBean(parentId: userId), 100, 0) as List<EmailAddressEntity>
        return emails ? emails.sort(true, { a,b -> a.isDefault <=> b.isDefault }).get(0).emailAddress : ''
    }

    private static ReportDataDto packReportTable(ReportTable reportTable)
    {
        ReportDataDto reportDataDto = new ReportDataDto()
        List<ReportTable> reportTables = new ArrayList<ReportTable>()
        reportTables.add(reportTable)
        reportDataDto.setTables(reportTables)
        return reportDataDto
    }

    private static Language getDefaultLanguage(){
        return new Language(id:  1)
    }

    @Override
    void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext
    }
}
