import pandas as pd

class DataExtractor:
    def __init__(self, invoices_path, expired_invoices_path):
        self.invoices_path = invoices_path
        self.expired_invoices_path = expired_invoices_path
        self.expired_invoices = self.load_expired_invoices()
        self.invoices_df = self.load_invoices()
        self.transformed_df = self.transform_data()

    def load_expired_invoices(self):
        # Load expired invoices
        with open(self.expired_invoices_path, 'r') as file:
            expired_invoices = file.read().split(',')
        return [int(invoice.strip()) for invoice in expired_invoices]

    def load_invoices(self):
        # Load new invoices
        invoices_new = pd.read_pickle(self.invoices_path)
        return pd.DataFrame(invoices_new)

    def transform_data(self):
        # Flatten the nested invoice data
        flattened_data = []
        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}

        for _, row in self.invoices_df.iterrows():
            invoice_id = int(row['id'])
            created_on = pd.to_datetime(row['created_on'])
            invoice_total = sum(item['item']['unit_price'] * item['quantity'] for item in row['items'])

            for item in row['items']:
                invoiceitem_id = item['item']['id']
                invoiceitem_name = item['item']['name']
                item_type = type_conversion[item['item']['type']]
                unit_price = item['item']['unit_price']
                quantity = item['quantity']
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / invoice_total
                is_expired = invoice_id in self.expired_invoices

                flattened_data.append({
                    'invoice_id': invoice_id,
                    'created_on': created_on,
                    'invoiceitem_id': invoiceitem_id,
                    'invoiceitem_name': invoiceitem_name,
                    'type': item_type,
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                })

        # Create a DataFrame from the flattened data
        transformed_df = pd.DataFrame(flattened_data)
        # Ensure the columns have the correct data types
        transformed_df['invoice_id'] = transformed_df['invoice_id'].astype(int)
        transformed_df['created_on'] = pd.to_datetime(transformed_df['created_on'])
        transformed_df['invoiceitem_id'] = transformed_df['invoiceitem_id'].astype(int)
        transformed_df['invoiceitem_name'] = transformed_df['invoiceitem_name'].astype(str)
        transformed_df['type'] = transformed_df['type'].astype(str)
        transformed_df['unit_price'] = transformed_df['unit_price'].astype(int)
        transformed_df['total_price'] = transformed_df['total_price'].astype(int)
        transformed_df['percentage_in_invoice'] = transformed_df['percentage_in_invoice'].astype(float)
        transformed_df['is_expired'] = transformed_df['is_expired'].astype(bool)

        # Sort the DataFrame by invoice_id and invoiceitem_id
        transformed_df.sort_values(by=['invoice_id', 'invoiceitem_id'], inplace=True)

        return transformed_df

    def get_transformed_data(self):
        return self.transformed_df

# Example usage
data_extractor = DataExtractor('invoices_new.pkl', 'expired_invoices.txt')
transformed_data = data_extractor.get_transformed_data()
transformed_data.to_csv('cleaned_invoices.csv', index=False)
