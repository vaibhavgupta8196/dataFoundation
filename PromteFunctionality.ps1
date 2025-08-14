[CmdletBinding()]

param (
    $cdo_datafdn_auditsql_servername
    , $cdo_datafdn_auditsql_databasename
    , $cdo_datafdn_auditsql_username
    , $cdo_datafdn_auditsql_password
    , $PAT
    , $DataAssetName
    , $ActionType
    , $DataAssetDetails 
    , $DataSetDetails
    , $BronzeMergeDetails
    , $BronzeRawDetails
    , $SilverDetails 
    , $RequestedBy
    , $Status
    , $ErrorMessage
    , $RequestedByEmail
    , $LogicAppsEndPoint
    , $Code
    , $IsDQIndependent
    , $IsHRD
    , $GoldDetails
)


$PortalURL = "https://cdo-datafdn-dev-app-consumption.intra.pepsico.com/"
Write-Host "Testing HRD Promote"
Write-Host "$IsHRD"
if ($IsHRD -eq "") {
    Write-Host "IsHRD param is empty"
}

if ($IsHRD -eq "y") {
    Write-Host "HRD DataAsset"
    try {
        Write-Host "$DataAssetDetails"
  
    
        if ("$($env:ActionType)" -eq "LowerEnvironment") {
            $DataAssetDetails = ""
            $QueryStatus = 0
            Write-Host "Lower Environment"
            Write-Host "$ActionType"
            Write-Host "$DataAssetName"
      
            # Update the lower environment table with Status and Error message if any
            $Query = "EXEC $($env:HRDSchemaName).UspPromoteHRDDataAsset  @DataAssetDetails = '" + $DataAssetDetails + "', @ActionType = '" + $ActionType + "', @DataAssetName = '" + $DataAssetName + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "'" 
            Write-Host "$Query"
        
            Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username  -Password $cdo_datafdn_auditsql_password -Verbose
        


            # Send Email Notification
            $EmailStatus = "Failed"
            $Subject = "HRD Data Asset $DataAssetName promotion status"
            if (("$Status" -eq "Update Successful") -or ("$Status" -eq "Insert Successful")) {
                $EmailStatus = "Successful"
            }



            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Content-Type", "application/json")

            $firstN = $RequestedByEmail.Split(".")[0]
            $lastN = $RequestedByEmail.Split(".")[1]
            $fullName = $firstN + " " + $lastN
            Write-Host $fullName
            $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"

        `n,`"subject`": `"$Subject`"
        `n,`"message`":`"$EmailStatus`"
        `n,`"ErrorMessage`":`"$ErrorMessage`"
        `n,`"portalUrl`": `"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"EmailType`":`"promoteDataAssetRequester`"}"

            $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        

        }
        else {
        
            Write-Host "HigherEnvironment"
            Write-Host "$ActionType"
            Write-Host "$DataAssetName"
            Write-Host "$DataAssetDetails"
        
            # Insert or update the pipeline request in higher environment table
            $Query = "EXEC $($env:HRDSchemaName).UspPromoteHRDDataAsset  @DataAssetDetails = '" + $DataAssetDetails + "', @ActionType = '" + $ActionType + "', @DataAssetName = '" + $DataAssetName + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "'" 
            Write-Host "$Query"
    
            $QueryStatus = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username -Password $cdo_datafdn_auditsql_password -Verbose 4>&1
            Write-Host "Stored Procedure executed"
            Write-Host "$QueryStatus"
            #Write-Host $QueryStatus.Item(0)

            # Call Lower Environment Pipeline with Status
        

            # Encode PAT Token

            $readableText = ':' + $PAT
        
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")

            #Fetching the Status and message from the output of the stored procedure
            $FinalStatus = $QueryStatus.Item("Status")
            $FinalMessage = $QueryStatus.Item("ErrorMessage")

            Write-Host $FinalStatus
            Write-Host $FinalMessage

            $body = "{
        `n    `"definition`": {
        `n        `"id`": $($env:ChildPipelineId)
        `n    },
        `n    `"variables`": {
        `n        `"DataAssetName`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$DataAssetName`"
        `n        },
        `n        `"ActionType`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"LowerEnvironment`"
        `n        },
        `n        `"RequestedBy`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedBy`"
        `n        },
        `n        `"RequestedByEmail`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedByEmail`"
        `n        },
        `n        `"Status`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalStatus`"
        `n        },

        `n        `"DataAssetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"DataSetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeRawDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeMergeDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"SilverDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"ErrorMessage`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalMessage`"
        `n        },
        `n         `"Code`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$Code`"
        `n        },
        `n         `"IsDQIndependent`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"n`"
        `n        },
        `n         `"IsHRD`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"y`"
        `n        },
        `n    }
        `n}"
            Write-Host "$body"
            # Invoking the child pipeline

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json

            # Send Email Notification To Higher Environment product owners:
            if (("$FinalStatus" -eq "Update Successful") -or ("$FinalStatus" -eq "Insert Successful")) {
                # Get Product Owner List

                $QueryToFetchOwners = "SELECT STRING_AGG(ISNULL(DIU.UserEmail,' '), ';') AS Owner FROM $($env:SchemaName).DimIngestionUser DIU JOIN $($env:SchemaName).IngestionUserMasterRoles IUMR ON DIU.GPID = IUMR.GPID WHERE IUMR.IsDEPO= 1 AND IUMR.ProductId= (SELECT BusinessGroupId FROM $($env:HRDSchemaName).DataAssetRequest WHERE DataAssetName = '$DataAssetName')"
                Write-Host "$QueryToFetchOwners"
                $OwnersList = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $QueryToFetchOwners -Username $cdo_datafdn_auditsql_username -Password $cdo_datafdn_auditsql_password -Verbose 4>&1

                Write-Host "Line!"
                Write-Host "Before"
                Write-Host "Type : "
                Write-Host $OwnersList.GetType()
                $Owners = $OwnersList.Item("Owner")
                Write-Host $Owners
                Write-Host "After"
                # $Owners = $OwnersList.Item("Owners")
                #`n,`"receiver`": `"$Owners`"

                # Send Email Notification To Higher Environment:
                $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
                $headers.Add("Content-Type", "application/json")
                $firstN = $RequestedByEmail.Split(".")[0]
                $lastN = $RequestedByEmail.Split(".")[1]
                $fullName = $firstN + " " + $lastN
                Write-Host $fullName
                $body = "{`"creatorName`":`"$fullName`"
            `n,`"DataAssetName`":`"$DataAssetName`"
            `n,`"dqEnvironment`":`"dev consumption`"
            `n,`"portalUrl`":`"$PortalURL`"
            
            `n,`"receiver`": `"$Owners`"
            `n,`"cc`": `"$RequestedByEmail`"
            `n,`"EmailType`":`"promoteDataAssetOwner`"}" 
                Write-Host $body


                $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
                $response | ConvertTo-Json
            }
        }
    }
    catch {
        # Send Email Notification To Higher Environment:
        if ("$ActionType" -eq "HigherEnvironment") {

            
            
            if ("$QueryStatus" -eq 0 ) {
                $Status = "Failure"
                $ErrorMessage = "Internal Server Error"
            }
            else {
                $Status = $QueryStatus.Item("Status")
                $ErrorMessage = $QueryStatus.Item("ErrorMessage")
            }
            $readableText = ':' + $PAT
            
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")


            $body = "{
            `n    `"definition`": {
            `n        `"id`": $($env:ChildPipelineId)
            `n    },
            `n    `"variables`": {
            `n        `"DataAssetName`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$DataAssetName`"
            `n        },
            `n        `"ActionType`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"LowerEnvironment`"
            `n        },
            `n        `"RequestedBy`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedBy`"
            `n        },
            `n        `"RequestedByEmail`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedByEmail`"
            `n        },
            `n        `"Status`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$Status`"
            `n        },

            `n        `"DataAssetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"DataSetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeRawDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeMergeDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"SilverDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"ErrorMessage`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$ErrorMessage`"
            `n        }
            `n         `"IsDQIndependent`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"n`"
            `n        },
            `n    }
            `n}"
            Write-Host "$body"
            # Invoking the child pipeline

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        }
        Write-Host $_
        $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
        $headers.Add("Content-Type", "application/json")

        $firstN = $RequestedByEmail.Split(".")[0]
        $lastN = $RequestedByEmail.Split(".")[1]
        $fullName = $firstN + " " + $lastN
        Write-Host $fullName
        $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"
        `n,`"dqEnvironment`":`"dev`"
        `n,`"portalUrl`":`"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"ErrorMessage`": `"$($env:ActionType)`"
        `n,`"EmailType`":`"promoteError`"}" 


        $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
        $response | ConvertTo-Json
    }
}
elseif ($IsDQIndependent -eq "n") {
    Write-Host "Hello DQ"
    try {
        Write-Host "$DataAssetDetails"
  
    
        if ("$($env:ActionType)" -eq "LowerEnvironment") {
            $DataAssetDetails = ""
            $DataSetDetails = ""
            $BronzeMergeDetails = ""
            $BronzeRawDetails = ""
            $SilverDetails = ""
            $QueryStatus = 0
            Write-Host "Lower Environment"
            Write-Host "$ActionType"
            Write-Host "$DataAssetName"
      
            # Update the lower environment table with Status and Error message if any
            $Query = "EXEC $($env:SchemaName).UspPromoteDataAsset  @DataAssetDetails = '" + $DataAssetDetails + "', @DataSetDetails = '" + $DataSetDetails + "', @BronzeRawDetails = '" + $BronzeRawDetails + "', @BronzeMergeDetails = '" + $BronzeMergeDetails + "', @SilverDetails = '" + $SilverDetails + "', @ActionType = '" + $ActionType + "', @DataAssetName = '" + $DataAssetName + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "'" 
            Write-Host "$Query"
        
            Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username  -Password $cdo_datafdn_auditsql_password -Verbose
        


            # Send Email Notification
            $EmailStatus = "Failed"
            $Subject = "Data Asset $DataAssetName promotion status"
            if (("$Status" -eq "Update Successful") -or ("$Status" -eq "Insert Successful")) {
                $EmailStatus = "Successful"
            }



            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Content-Type", "application/json")

            $firstN = $RequestedByEmail.Split(".")[0]
            $lastN = $RequestedByEmail.Split(".")[1]
            $fullName = $firstN + " " + $lastN
            Write-Host $fullName
            $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"

        `n,`"subject`": `"$Subject`"
        `n,`"message`":`"$EmailStatus`"
        `n,`"ErrorMessage`":`"$ErrorMessage`"
        `n,`"portalUrl`": `"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"EmailType`":`"promoteDataAssetRequester`"}"

            $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        

        }
        else {
        
            Write-Host "HigherEnvironment"
            Write-Host "$ActionType"
            Write-Host "$DataAssetName"
            Write-Host "$DataAssetDetails"
        
            # Insert or update the pipeline request in higher environment table
            $Query = "EXEC $($env:SchemaName).UspPromoteDataAsset @DataAssetDetails = '" + $DataAssetDetails + "', @DataSetDetails = '" + $DataSetDetails + "', @BronzeRawDetails = '" + $BronzeRawDetails + "', @BronzeMergeDetails = '" + $BronzeMergeDetails + "', @SilverDetails = '" + $SilverDetails + "', @ActionType = '" + $ActionType + "', @DataAssetName = '" + $DataAssetName + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "'" 
            Write-Host "$Query"
    
            $QueryStatus = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username -Password $cdo_datafdn_auditsql_password -Verbose 4>&1
            Write-Host "Stored Procedure executed"
            Write-Host "$QueryStatus"
            #Write-Host $QueryStatus.Item(0)

            # Call Lower Environment Pipeline with Status
        

            # Encode PAT Token

            $readableText = ':' + $PAT
        
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")

            #Fetching the Status and message from the output of the stored procedure
            $FinalStatus = $QueryStatus.Item("Status")
            $FinalMessage = $QueryStatus.Item("ErrorMessage")

            Write-Host $FinalStatus
            Write-Host $FinalMessage

            $body = "{
        `n    `"definition`": {
        `n        `"id`": $($env:ChildPipelineId)
        `n    },
        `n    `"variables`": {
        `n        `"DataAssetName`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$DataAssetName`"
        `n        },
        `n        `"ActionType`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"LowerEnvironment`"
        `n        },
        `n        `"RequestedBy`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedBy`"
        `n        },
        `n        `"RequestedByEmail`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedByEmail`"
        `n        },
        `n        `"Status`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalStatus`"
        `n        },

        `n        `"DataAssetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"DataSetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeRawDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeMergeDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"SilverDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"ErrorMessage`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalMessage`"
        `n        },
        `n         `"Code`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$Code`"
        `n        },
        `n         `"IsDQIndependent`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"n`"
        `n        },
        `n    }
        `n}"
            Write-Host "$body"
            # Invoking the child pipeline

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json

            # Send Email Notification To Higher Environment product owners:
            if (("$FinalStatus" -eq "Update Successful") -or ("$FinalStatus" -eq "Insert Successful")) {
                # Get Product Owner List

                $QueryToFetchOwners = "SELECT STRING_AGG( ISNULL(DIU.UserEmail, ' '), ';') As Owners FROM datafdn.DimIngestionUser DIU JOIN datafdn.IngestionUserMasterRoles IUMR ON DIU.GPID = IUMR.GPID WHERE IUMR.IsDEPO = 1 AND IUMR.ProductId = (SELECT BusinessGroupId FROM datafdn.DataAssetRequest WHERE DataAssetName = '$DataAssetName')"
                $OwnersList = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $QueryToFetchOwners -Username $cdo_datafdn_auditsql_username -Password $cdo_datafdn_auditsql_password -Verbose 4>&1

                Write-Host "Line!"
                Write-Host $OwnersList.Item("Owners")

                $Owners = $OwnersList.Item("Owners")
                #`n,`"receiver`": `"$Owners`"

                # Send Email Notification To Higher Environment:
                $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
                $headers.Add("Content-Type", "application/json")
                $firstN = $RequestedByEmail.Split(".")[0]
                $lastN = $RequestedByEmail.Split(".")[1]
                $fullName = $firstN + " " + $lastN
                Write-Host $fullName
                $body = "{`"creatorName`":`"$fullName`"
            `n,`"DataAssetName`":`"$DataAssetName`"
            `n,`"dqEnvironment`":`"dev`"
            `n,`"portalUrl`":`"$PortalURL`"
            
            `n,`"receiver`": `"$Owners`"
            `n,`"cc`": `"$RequestedByEmail`"
            `n,`"EmailType`":`"promoteDataAssetOwner`"}" 
                Write-Host $body


                $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
                $response | ConvertTo-Json
            }
        }
    }
    catch {
        # Send Email Notification To Higher Environment:
        if ("$ActionType" -eq "HigherEnvironment") {

            
            
            if ("$QueryStatus" -eq 0 ) {
                $Status = "Failure"
                $ErrorMessage = "Internal Server Error"
            }
            else {
                $Status = $QueryStatus.Item("Status")
                $ErrorMessage = $QueryStatus.Item("ErrorMessage")
            }
            $readableText = ':' + $PAT
            
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")


            $body = "{
            `n    `"definition`": {
            `n        `"id`": $($env:ChildPipelineId)
            `n    },
            `n    `"variables`": {
            `n        `"DataAssetName`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$DataAssetName`"
            `n        },
            `n        `"ActionType`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"LowerEnvironment`"
            `n        },
            `n        `"RequestedBy`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedBy`"
            `n        },
            `n        `"RequestedByEmail`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedByEmail`"
            `n        },
            `n        `"Status`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$Status`"
            `n        },

            `n        `"DataAssetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"DataSetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeRawDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeMergeDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"SilverDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"ErrorMessage`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$ErrorMessage`"
            `n        }
            `n         `"IsDQIndependent`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"n`"
            `n        },
            `n    }
            `n}"
            Write-Host "$body"
            # Invoking the child pipeline

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        }
        Write-Host $_
        $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
        $headers.Add("Content-Type", "application/json")

        $firstN = $RequestedByEmail.Split(".")[0]
        $lastN = $RequestedByEmail.Split(".")[1]
        $fullName = $firstN + " " + $lastN
        Write-Host $fullName
        $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"
        `n,`"dqEnvironment`":`"dev`"
        `n,`"portalUrl`":`"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"ErrorMessage`": `"$($env:ActionType)`"
        `n,`"EmailType`":`"promoteError`"}" 


        $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
        $response | ConvertTo-Json
    }
}
elseif ($IsDQIndependent -eq "y") {
   
    try {
        Write-Host "Hello DQ"
        if ("$($env:ActionType)" -eq "LowerEnvironment") {

            $QueryStatus = 0 
            $DataSetDetails = ""
            $DataSetValidationDetails = ""
            $Code = "$Code"
            Write-Host "Lower Environment"
            Write-Host "$ActionType"
            Write-Host "$Param"
            # Update the lower environment table with Status and Error message if any
            $Query = "EXEC $($env:SchemaName).UspPromoteIndependentDQ @DataSetDetails = '" + $DataSetDetails + "', @BronzeRawDetails = '" + $BronzeRawDetails + "', @BronzeMergeDetails = '" + $BronzeMergeDetails + "', @SilverDetails = '" + $SilverDetails + "' ,@GoldDetails = '" + $GoldDetails + "' , @ActionType = '" + $ActionType + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "' , @Code = '" + $Code + "', @DataSetName = '" + $DataAssetName + "'"
            Write-Host "$Query"
            $QueryStatus = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username  -Password $cdo_datafdn_auditsql_password -Verbose

 

            # Send Email Notification
            $EmailStatus = "Failed"
            $Subject = "Data Set $DataAssetName promotion status"
            if (("$Status" -eq "Update Successful") -or ("$Status" -eq "Insert Successful") -or ("$Status" -eq "Insert Or Update Successful")) {
                $EmailStatus = "Successful"
            }



            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Content-Type", "application/json")

            $firstN = $RequestedByEmail.Split(".")[0]
            $lastN = $RequestedByEmail.Split(".")[1]
            $fullName = $firstN + " " + $lastN
            Write-Host $fullName
            $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"

        `n,`"subject`": `"$Subject`"
        `n,`"message`":`"$EmailStatus`"
        `n,`"ErrorMessage`":`"$ErrorMessage`"
        `n,`"portalUrl`": `"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"EmailType`":`"promoteDatasetRequester`"}"

            $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        

        }
        else {
 
            # Write-Host "$DataSetDetails"
            $Query = "EXEC $($env:SchemaName).UspPromoteIndependentDQ @DataSetDetails = '" + $DataSetDetails + "', @BronzeRawDetails = '" + $BronzeRawDetails + "', @BronzeMergeDetails = '" + $BronzeMergeDetails + "', @SilverDetails = '" + $SilverDetails + "' ,@GoldDetails = '" + $GoldDetails + "' , @ActionType = '" + $ActionType + "', @Status = '" + $Status + "', @ErrorMessage = '" + $ErrorMessage + "' , @Code = '" + $Code + "', @DataSetName = '" + $DataAssetName + "'"
            Write-Host "$Query"
            $QueryStatus = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $Query -Username $cdo_datafdn_auditsql_username  -Password $cdo_datafdn_auditsql_password -Verbose
            Write-Host "After invoke"
            $readableText = ':' + $PAT
            Write-Host "after sql"
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
        
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)

            Write-Host "After PatToken"
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")
            Write-Host $QueryStatus 
            #Fetching the Status and message from the output of the stored procedure
            $FinalStatus = $QueryStatus.Item("Status")
            Write-Host $FinalStatus
            $FinalMessage = $QueryStatus.Item("ErrorMessage")
            Write-Host $FinalMessage

            $body = "{
        `n    `"definition`": {
        `n        `"id`": $($env:ChildPipelineId)
        `n    },
        `n    `"variables`": {
        `n        `"DataAssetName`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$DataAssetName`"
        `n        },
        `n        `"ActionType`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"LowerEnvironment`"
        `n        },
        `n        `"RequestedBy`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedBy`"
        `n        },
        `n        `"RequestedByEmail`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$RequestedByEmail`"
        `n        },
        `n        `"Status`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalStatus`"
        `n        },

        `n        `"DataAssetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"DataSetDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeRawDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"BronzeMergeDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"SilverDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n         `"GoldDetails`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"`"
        `n        },
        `n        `"ErrorMessage`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$FinalMessage`"
        `n        },
        `n         `"Code`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"$Code`"
        `n        },
        `n         `"IsDQIndependent`": {
        `n            `"isSecret`": false,
        `n            `"value`": `"y`"
        `n        },
        `n    }
        `n}"
            Write-Host "Before body"
            Write-Host "$body"

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json

            # Send Email Notification To Higher Environment product owners:
            if (("$FinalStatus" -eq "Update Successful") -or ("$FinalStatus" -eq "Insert Successful") -or ("$FinalStatus" -eq "Insert Or Update Successful")) {
                # Get Product Owner List

                $QueryToFetchOwners = "SELECT STRING_AGG( ISNULL(DIU.UserEmail, ' '), ';') As Owners FROM $($env:SchemaName).DimIngestionUser DIU JOIN $($env:SchemaName).IngestionUserMasterRoles IUMR ON DIU.GPID = IUMR.GPID WHERE IUMR.IsDEPO = 1 AND IUMR.ProductId = (SELECT BusinessGroupId FROM $($env:SchemaName).DataAssetRequest WHERE DataAssetName = '$DataAssetName')"
                $OwnersList = Invoke-Sqlcmd -ServerInstance $cdo_datafdn_auditsql_servername -Database $cdo_datafdn_auditsql_databasename -Query $QueryToFetchOwners -Username $cdo_datafdn_auditsql_username -Password $cdo_datafdn_auditsql_password -Verbose 4>&1

                Write-Host "Line!"
                Write-Host $OwnersList.Item("Owners")

                $Owners = $OwnersList.Item("Owners")
                #`n,`"receiver`": `"$Owners`"

                # Send Email Notification To Higher Environment:
                $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
                $headers.Add("Content-Type", "application/json")

                $firstN = $RequestedByEmail.Split(".")[0]
                $lastN = $RequestedByEmail.Split(".")[1]
                $fullName = $firstN + " " + $lastN
                Write-Host $fullName
                $body = "{`"creatorName`":`"$fullName`"
            `n,`"DataAssetName`":`"$DataAssetName`"
            `n,`"dqEnvironment`":`"dev`"
            `n,`"portalUrl`":`"$PortalURL`"
            
            `n,`"receiver`": `"$Owners`"
            `n,`"cc`": `"$RequestedByEmail`"
            `n,`"EmailType`":`"promoteDatasetOwner`"}" 
                Write-Host $body


                $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
                $response | ConvertTo-Json
            }
        }
    }
    catch {
        if ("$ActionType" -eq "HigherEnvironment") {

            if ("$QueryStatus" -eq 0 ) {
                $Status = "Failure"
                $ErrorMessage = "Internal Server Error"
            }
            else {
                $Status = $QueryStatus.Item("Status")
                $ErrorMessage = $QueryStatus.Item("ErrorMessage")
            }
            $readableText = ':' + $PAT
            
            $encodedBytes = [System.Text.Encoding]::UTF8.GetBytes($readableText)
            $EncodedPatToken = [System.Convert]::ToBase64String($encodedBytes)
            # Creating a connection to lower environment pipeline
            $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
            $headers.Add("Authorization", "Basic $EncodedPatToken")
            $headers.Add("Content-Type", "application/json")
            $headers.Add("Cookie", "VstsSession=%7B%22PersistentSessionId%22%3A%22bac7bfea-7576-4d8f-baa5-cccf285ec31c%22%2C%22PendingAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22CurrentAuthenticationSessionId%22%3A%2200000000-0000-0000-0000-000000000000%22%2C%22SignInState%22%3A%7B%7D%7D")


            $body = "{
            `n    `"definition`": {
            `n        `"id`": $($env:ChildPipelineId)
            `n    },
            `n    `"variables`": {
            `n        `"DataAssetName`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$DataAssetName`"
            `n        },
            `n        `"ActionType`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"LowerEnvironment`"
            `n        },
            `n        `"RequestedBy`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedBy`"
            `n        },
            `n        `"RequestedByEmail`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$RequestedByEmail`"
            `n        },
            `n        `"Status`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$Status`"
            `n        },

            `n        `"DataAssetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"DataSetDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeRawDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"BronzeMergeDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"SilverDetails`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"`"
            `n        },
            `n        `"ErrorMessage`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"$ErrorMessage`"
            `n        }
            `n         `"IsDQIndependent`": {
            `n            `"isSecret`": false,
            `n            `"value`": `"y`"
            `n        },
            `n    }
            `n}"
            Write-Host "$body"
            # Invoking the child pipeline

            $url = 'https://dev.azure.com/PepsiCoIT/Global_Data_Project/_apis/pipelines/' + $($env:ChildPipelineId) + '/runs?api-version=6.0-preview.1'
            $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body
            $response | ConvertTo-Json
        }
        Write-Host $_
        $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
        $headers.Add("Content-Type", "application/json")

        $firstN = $RequestedByEmail.Split(".")[0]
        $lastN = $RequestedByEmail.Split(".")[1]
        $fullName = $firstN + " " + $lastN
        Write-Host $fullName
        $body = "{`"creatorName`":`"$fullName`"
        `n,`"DataAssetName`":`"$DataAssetName`"
        `n,`"dqEnvironment`":`"dev consumption`"
        `n,`"portalUrl`":`"$PortalURL`"
        
        `n,`"receiver`": `"$RequestedByEmail`"
        `n,`"ErrorMessage`": `"$($env:ActionType)`"
        `n,`"EmailType`":`"promoteDatasetError`"}" 


        $response = Invoke-RestMethod $LogicAppsEndPoint -Method 'POST' -Headers $headers -Body $body
        $response | ConvertTo-Json
    }
}