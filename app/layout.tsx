export const metadata = {
  title: "Compound Indexing API",
  description: "Backend API service for Compound wallet indexing",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}