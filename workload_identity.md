# How to Establish Workload Identity

Follow these steps to set up workload identity on your system:

1. **Prerequisites**
   - Ensure you have Azure CLI installed.
   - You need access to an Azure subscription and resource group.
   - Install kubectl if working with Kubernetes.

2. **Create a User-Assigned Managed Identity**
   - You need to use a user-assigned managed identity for workload identity integration.
   - ```bash
     az identity create --name <identity-name> --resource-group <resource-group>
     ```

3. **Assign Required Roles**
   - You need to assign the Contributor role to the managed identity:
     ```bash
     az role assignment create --assignee <identity-client-id> --role Contributor --scope <scope>
     ```
   - You may also assign other roles as needed for your use case.

4. **Configure Workload Identity Federation (for Kubernetes)**
   - Enable OIDC issuer on your AKS cluster:
     ```bash
     az aks update --name <aks-cluster> --resource-group <resource-group> --enable-oidc-issuer
     ```
   - Get the OIDC issuer URL:
     ```bash
     az aks show --name <aks-cluster> --resource-group <resource-group> --query "oidcIssuerProfile.issuerUrl" -o tsv
     ```

5. **Create a Kubernetes Service Account**
   - Create a service account in your desired namespace:
     ```bash
     kubectl create serviceaccount <service-account-name> --namespace <namespace>
     ```
   - This service account will be referenced in the subject claim for federated credentials.
   - Annotate the service account with the managed identity client ID:
     ```bash
     kubectl annotate serviceaccount <service-account-name> --namespace <namespace> azure.workload.identity/client-id=<identity-client-id>
     ```

6. **Create Federated Credential**
   - The subject claim identifies the Kubernetes service account that will use the federated credential. It typically follows the format: "system:serviceaccount:<namespace>:<service-account-name>".
   - Example subject claim: system:serviceaccount:default:my-service-account
   - ```bash
     az identity federated-credential create --name <credential-name> --identity-name <identity-name> --resource-group <resource-group> --issuer <oidc-issuer-url> --subject <subject-claim>
     ```

7. **Configure Your Application**
   - Update your application to use the managed identity and federated credential for authentication.
   - Set environment variables or configuration files as needed.

8. **Troubleshooting**
   - Check Azure portal for identity and role assignments.
   - Use Azure CLI logs for debugging.

9. **Build and Push Docker Image**
   - Build your Docker image:
     ```bash
     docker build -t <registry-name>.azurecr.io/<image-name>:<tag> .
     ```
   - Log in to Azure Container Registry:
     ```bash
     az acr login --name <registry-name>
     ```
   - Push the image to the registry:
     ```bash
     docker push <registry-name>.azurecr.io/<image-name>:<tag>
     ```

10. **Create and Configure Kubernetes Deployment**
   - Create a deployment YAML file (e.g., deployment.yaml) referencing your image and service account:
     ```yaml
     apiVersion: apps/v1
     kind: Deployment
     metadata:
       name: <deployment-name>
       namespace: <namespace-name>
     spec:
       replicas: 1
       selector:
         matchLabels:
           app: <app-label>
       template:
         metadata:
           labels:
             app: <app-label>
             azure.workload.identity/use: "true" 
         spec:
           serviceAccountName: <service-account-name>
           containers:
             - name: <container-name>
               image: <registry-name>.azurecr.io/<image-name>:<tag>
               env:
                 - name: CLIENT_ID
                   value: <workload_identity_client_id>
                 - name: TENANT_ID
                   value: <workload_identity_tenant_id>
               volumeMounts:
                 - name: azure-identity-token
                   mountPath: /var/run/secrets/azure/tokens
                   readOnly: true
           volumes:
             - name: azure-identity-token
               projected:
                 sources:
                   - serviceAccountToken:
                       path: azure-identity-token
                       audience: api://AzureADTokenExchange
                       expirationSeconds: 3600
     ```
   - Apply the deployment:
     ```bash
     kubectl apply -f deployment.yaml
     ```

11. **Check Pods and Logs**
   - List pods:
     ```bash
     kubectl get pods --namespace <namespace>
     ```
   - Check pod logs:
     ```bash
     kubectl logs <pod-name> --namespace <namespace>
     ```

Replace placeholders (e.g., <identity-name>, <resource-group>) with your actual values.

For more details, refer to Azure documentation on [Workload Identity](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview).