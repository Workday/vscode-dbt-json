export function Text({
  children,
  variant,
}: React.PropsWithChildren<{ variant?: 'label' }>) {
  switch (variant) {
    case 'label': {
      return <label>{children}</label>;
    }
    default: {
      return <p>{children}</p>;
    }
  }
}
