package jobworker

import "testing"

func TestJob_IsFinished(t *testing.T) {
	type fields struct {
		didSomething uint32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "finished",
			fields: fields{
				didSomething: 1,
			},
			want: true,
		},
		{
			name: "no finished",
			fields: fields{
				didSomething: 0,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				didSomething: tt.fields.didSomething,
			}
			if got := j.IsFinished(); got != tt.want {
				t.Errorf("Job.IsFinished() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_finished(t *testing.T) {
	type fields struct {
		didSomething uint32
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "finished",
			fields: fields{
				didSomething: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{
				didSomething: tt.fields.didSomething,
			}
			j.finished()
			if j.didSomething != 1 {
				t.Errorf("Job.IsFinished() = %v, want %v", j.didSomething, 1)
			}
		})
	}
}
